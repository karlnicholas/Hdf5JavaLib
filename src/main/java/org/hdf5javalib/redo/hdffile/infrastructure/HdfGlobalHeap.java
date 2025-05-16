package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.HdfFileAllocation;
import org.hdf5javalib.redo.utils.HdfReadUtils;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Manages HDF5 global heap collections as defined in the HDF5 specification.
 * <p>
 * The {@code HdfGlobalHeap} class handles the storage and retrieval of global heap objects
 * in an HDF5 file. It supports reading and writing heap collections, adding new objects,
 * and managing object IDs and sizes. Each heap collection is identified by its file offset
 * and contains objects with unique IDs and associated data.
 * </p>
 *
 * @see HdfDataFile
 * @see HdfFixedPoint
 * @see HdfFileAllocation
 */
public class HdfGlobalHeap {
    private static final String SIGNATURE = "GCOL";
    private static final int VERSION = 1;

    /** Map of heap offsets to collections of global heap objects. */
    private final Map<HdfFixedPoint, LinkedHashMap<Integer, GlobalHeapObject>> heapCollections;
    /** Map of heap offsets to their declared sizes. */
    private final Map<HdfFixedPoint, HdfFixedPoint> collectionSizes;
    /** Map of heap offsets to the next available object ID. */
    private final Map<HdfFixedPoint, Integer> nextObjectIds;
    /** The current heap offset for writing new objects. */
    private HdfFixedPoint currentWriteHeapOffset;
    /** Optional initializer for lazy loading heap collections. */
    private final GlobalHeapInitialize initialize;
    /** The HDF5 file context. */
    private final HdfDataFile hdfDataFile;

    /**
     * Constructs an HdfGlobalHeap with an initializer and file context.
     *
     * @param initialize the initializer for lazy loading heap collections
     * @param hdfDataFile   the HDF5 file context
     */
    public HdfGlobalHeap(GlobalHeapInitialize initialize, HdfDataFile hdfDataFile) {
        this.initialize = initialize;
        this.hdfDataFile = hdfDataFile;
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
        this.currentWriteHeapOffset = hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().undefined();
    }

    /**
     * Constructs an HdfGlobalHeap without an initializer.
     *
     * @param hdfDataFile the HDF5 file context
     */
    public HdfGlobalHeap(HdfDataFile hdfDataFile) {
        this.hdfDataFile = hdfDataFile;
        this.initialize = null;
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
        this.currentWriteHeapOffset = hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().undefined();
    }

    /**
     * Retrieves the data bytes for a specific global heap object.
     *
     * @param heapOffset the offset of the heap collection
     * @param objectId   the ID of the object
     * @return the data bytes of the object
     * @throws IllegalArgumentException if the object ID is 0 or invalid
     * @throws IllegalStateException if the heap or object is not found
     */
    public byte[] getDataBytes(long heapOffset, int objectId) {
        if (objectId == 0) {
            throw new IllegalArgumentException("Cannot request data bytes for Global Heap Object ID 0 (null terminator)");
        }
        LinkedHashMap<Integer, GlobalHeapObject> specificHeapObjects = heapCollections.get(heapOffset);
        if (specificHeapObjects == null) {
            if (initialize != null) {
                initialize.initializeCallback(heapOffset);
                specificHeapObjects = heapCollections.get(heapOffset);
                if (specificHeapObjects == null) {
                    throw new IllegalStateException("Heap not found or loaded for offset: " + heapOffset + " even after initialization callback.");
                }
            } else {
                throw new IllegalStateException("Heap not loaded for offset: " + heapOffset + " and no initializer provided.");
            }
        }
        GlobalHeapObject obj = specificHeapObjects.get(objectId);
        if (obj == null) {
            throw new RuntimeException("No object found for objectId: " + objectId + " in heap at offset: " + heapOffset);
        }
        if (obj.getHeapObjectIndex() == 0) {
            throw new RuntimeException("Internal error: Object ID 0 found unexpectedly during data retrieval for offset: " + heapOffset);
        }
        return obj.getData();
    }

    /**
     * Reads a global heap collection from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @throws IOException if an I/O error occurs or the heap data is invalid
     */
    public void readFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws IOException {
        long startOffset = fileChannel.position();
        ByteBuffer headerBuffer = ByteBuffer.allocate(16);
        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        int bytesRead = fileChannel.read(headerBuffer);
        if (bytesRead < 16) {
            throw new IOException("Failed to read complete global heap header at offset: " + startOffset);
        }
        headerBuffer.flip();

        byte[] signatureBytes = new byte[4];
        headerBuffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!SIGNATURE.equals(signature)) {
            throw new IllegalArgumentException("Invalid global heap signature: '" + signature + "' at offset: " + startOffset);
        }

        int version = Byte.toUnsignedInt(headerBuffer.get());
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported global heap version: " + version + " at offset: " + startOffset);
        }

        headerBuffer.position(headerBuffer.position() + 3);
        HdfFixedPoint localCollectionSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), headerBuffer);
        long declaredSize = localCollectionSize.getInstance(Long.class);

        if (declaredSize < 16 || (startOffset + declaredSize > fileChannel.size())) {
            if (startOffset + declaredSize > fileChannel.size() && declaredSize >= 16) {
                throw new IllegalArgumentException("Declared collection size " + declaredSize + " at offset " + startOffset + " exceeds file size " + fileChannel.size());
            } else if (declaredSize < 16) {
                throw new IllegalArgumentException("Declared collection size " + declaredSize + " is less than minimum header size (16) at offset " + startOffset);
            }
        }

        int objectDataBufferSize = (int) (declaredSize - 16);
        if (objectDataBufferSize < 0) {
            if (declaredSize > 16 && objectDataBufferSize < 16) {
                throw new IllegalArgumentException("Declared size " + declaredSize + " at offset " + startOffset + " implies insufficient space ("+objectDataBufferSize+" bytes) for null terminator object.");
            } else if (objectDataBufferSize < 0) {
                throw new IllegalArgumentException("Declared size " + declaredSize + " is too small for heap header at offset " + startOffset);
            }
        }

        ByteBuffer objectBuffer = null;
        if (objectDataBufferSize > 0) {
            objectBuffer = ByteBuffer.allocate(objectDataBufferSize);
            objectBuffer.order(ByteOrder.LITTLE_ENDIAN);
            bytesRead = fileChannel.read(objectBuffer);
            if (bytesRead < objectDataBufferSize) {
                throw new IOException("Failed to read complete global heap object data buffer ("+bytesRead+"/"+objectDataBufferSize+") at offset: " + startOffset);
            }
            objectBuffer.flip();
        }

        LinkedHashMap<Integer, GlobalHeapObject> localObjects = new LinkedHashMap<>();
        int localNextObjectId = 1;

        try {
            if (objectBuffer != null) {
                while (objectBuffer.hasRemaining()) {
                    if (objectBuffer.remaining() < 16) {
                        break;
                    }
                    GlobalHeapObject obj = GlobalHeapObject.readFromByteBuffer(objectBuffer);
                    if (obj.getHeapObjectIndex() == 0) {
                        localObjects.put(obj.getHeapObjectIndex(), obj);
                        break;
                    }
                    if (localObjects.containsKey(obj.getHeapObjectIndex())) {
                        throw new RuntimeException("Duplicate object ID " + obj.getHeapObjectIndex() + " found in heap at offset: " + startOffset);
                    }
                    if (obj.getHeapObjectIndex() < 1 || obj.getHeapObjectIndex() > 0xFFFF) {
                        throw new RuntimeException("Invalid object ID " + obj.getHeapObjectIndex() + " found in heap at offset: " + startOffset);
                    }
                    localObjects.put(obj.getHeapObjectIndex(), obj);
                    localNextObjectId = Math.max(localNextObjectId, obj.getHeapObjectIndex() + 1);
                }
            }
        } catch (Exception e) {
            throw new IOException("Unexpected error processing global heap object data buffer at offset: " + startOffset, e);
        }

        HdfFixedPoint hdfStartOffset = HdfWriteUtils.hdfFixedPointFromValue(startOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset());
        this.heapCollections.put(hdfStartOffset, localObjects);
        this.collectionSizes.put(hdfStartOffset, localCollectionSize);
        this.nextObjectIds.put(hdfStartOffset, localNextObjectId);
    }

    /**
     * Adds a byte array to the global heap and returns a reference to it.
     *
     * @param bytes the byte array to add
     * @return a byte array containing the heap offset and object ID
     * @throws IllegalArgumentException if the input byte array is null
     * @throws IllegalStateException if the heap block is not allocated or full
     */
    public byte[] addToHeap(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Input byte array cannot be null.");
        }

        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();

        if (currentWriteHeapOffset.isUndefined()) {
            this.currentWriteHeapOffset = fileAllocation.getGlobalHeapOffset();
        }
        HdfFixedPoint currentHeapOffset = this.currentWriteHeapOffset;

        LinkedHashMap<Integer, GlobalHeapObject> targetObjects = heapCollections.computeIfAbsent(currentHeapOffset, k -> new LinkedHashMap<>());

        long currentUsedSize = 16;
        for (GlobalHeapObject existingObj : targetObjects.values()) {
            currentUsedSize += 16;
            long existingObjectSize = existingObj.getObjectSize();
            if (existingObjectSize > Integer.MAX_VALUE) {
                throw new IllegalStateException("Existing object " + existingObj.getHeapObjectIndex() + " size too large to calculate padding.");
            }
            currentUsedSize += existingObjectSize;
            currentUsedSize += getPadding((int) existingObjectSize);
        }

        int newObjectDataSize = bytes.length;
        int newObjectPadding = getPadding(newObjectDataSize);
        long newObjectRequiredSize = 16L + newObjectDataSize + newObjectPadding;

        HdfFixedPoint blockSize = fileAllocation.getGlobalHeapBlockSize(currentHeapOffset);
        if (currentUsedSize + newObjectRequiredSize + 16L > blockSize) {
            // Add null terminator to mark the block as full
            long freeSpace = blockSize - currentUsedSize;
            if (freeSpace < 16) {
                throw new IllegalStateException("Insufficient space for null terminator in heap at offset " + currentHeapOffset);
            }
            if (!targetObjects.containsKey(0)) {
                GlobalHeapObject nullTerminator = new GlobalHeapObject(0, 0, freeSpace, null);
                targetObjects.put(0, nullTerminator);
            }

            HdfFixedPoint newHeapOffset;
            if (currentHeapOffset == fileAllocation.getGlobalHeapOffset()) {
                newHeapOffset = fileAllocation.allocateNextGlobalHeapBlock();
            } else {
                newHeapOffset = fileAllocation.expandGlobalHeapBlock();
            }
            this.currentWriteHeapOffset = newHeapOffset;
            currentHeapOffset = newHeapOffset;
            targetObjects = heapCollections.computeIfAbsent(currentHeapOffset, k -> new LinkedHashMap<>());
        } else if (targetObjects.containsKey(0)) {
            // Remove null terminator to allow new object insertion
            targetObjects.remove(0);
        }

        int currentNextId = nextObjectIds.getOrDefault(currentHeapOffset, 1);
        if (currentNextId > 0xFFFF) {
            throw new IllegalStateException("Maximum number of global heap objects (65535) exceeded for heap at offset " + currentHeapOffset);
        }

        GlobalHeapObject obj = new GlobalHeapObject(currentNextId, 0, newObjectDataSize, bytes);
        targetObjects.put(currentNextId, obj);
        int objectId = currentNextId;
        nextObjectIds.put(currentHeapOffset, currentNextId + 1);

        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(newObjectDataSize);
        buffer.putLong(currentHeapOffset);
        buffer.putInt(objectId);
        return buffer.array();
    }

    /**
     * Writes all global heap collections to a file channel.
     *
     * @param fileChannel the file channel to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
        if (heapCollections.isEmpty()) {
            return;
        }

        for (Map.Entry<Long, LinkedHashMap<Integer, GlobalHeapObject>> entry : heapCollections.entrySet()) {
            long heapOffset = entry.getKey();
            LinkedHashMap<Integer, GlobalHeapObject> objects = entry.getValue();

            long heapSize = this.hdfDataFile.getFileAllocation().getGlobalHeapBlockSize(heapOffset);
            fileChannel.position(heapOffset);
            int size1 = (int) getWriteBufferSize(heapOffset);
            ByteBuffer buffer = ByteBuffer.allocate((int)heapSize);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            buffer.put(SIGNATURE.getBytes());
            buffer.put((byte) VERSION);
            buffer.put(new byte[3]);
            buffer.putLong(calculateAlignedTotalSize(heapOffset));

            for (GlobalHeapObject obj : objects.values()) {
                obj.writeToByteBuffer(buffer);
            }

            if (!objects.containsKey(0)) {
                long usedSize = 16;
                for (GlobalHeapObject obj : objects.values()) {
                    long objSize = obj.getHeapObjectIndex() == 0 ? 16 : 16 + obj.getObjectSize() + getPadding((int) obj.getObjectSize());
                    usedSize += objSize;
                }
                long blockSize = hdfDataFile.getFileAllocation().getGlobalHeapBlockSize(heapOffset);
                long remainingSize = blockSize - usedSize;
                if (remainingSize < 16) {
                    throw new IllegalStateException("Insufficient space for null terminator in heap at offset: " + heapOffset);
                }
                buffer.putShort((short) 0);
                buffer.putShort((short) 0);
                buffer.putInt(0);
                buffer.putLong(remainingSize);
            }

            buffer.rewind();
            fileChannel.write(buffer);
        }
    }

    /**
     * Calculates the aligned total size of a heap collection.
     *
     * @param heapOffset the offset of the heap collection
     * @return the aligned total size in bytes
     * @throws IllegalStateException if the heap collection is not found
     */
    private long calculateAlignedTotalSize(long heapOffset) {
        LinkedHashMap<Integer, GlobalHeapObject> objects = heapCollections.get(heapOffset);
        if (objects == null) {
            throw new IllegalStateException("No heap collection found at offset: " + heapOffset);
        }

        long totalSize = 16;
        for (GlobalHeapObject obj : objects.values()) {
            long objSize = obj.getHeapObjectIndex() == 0 ? 16 : 16 + obj.getObjectSize() + getPadding((int) obj.getObjectSize());
            totalSize += objSize;
        }

        if (!objects.containsKey(0)) {
            totalSize += 16;
        }

        long blockSize = hdfDataFile.getFileAllocation().getGlobalHeapBlockSize(heapOffset);
        return alignTo(totalSize, (int) blockSize);
    }

    /**
     * Returns the size of the write buffer needed for a heap collection.
     *
     * @param heapOffset the offset of the heap collection
     * @return the buffer size in bytes
     */
    public long getWriteBufferSize(long heapOffset) {
        return calculateAlignedTotalSize(heapOffset);
    }

    /**
     * Aligns a size to the specified alignment boundary.
     *
     * @param size      the size to align
     * @param alignment the alignment boundary (must be a power of 2)
     * @return the aligned size
     * @throws IllegalArgumentException if the alignment is not a positive power of 2
     */
    private static long alignTo(long size, int alignment) {
        if (alignment <= 0 || (alignment & (alignment - 1)) != 0) {
            throw new IllegalArgumentException("Alignment must be a positive power of 2. Got: " + alignment);
        }
        return (size + alignment - 1) & ~((long) alignment - 1);
    }

    /**
     * Calculates the padding needed for a data size to align to an 8-byte boundary.
     *
     * @param size the data size
     * @return the padding size in bytes
     */
    private static int getPadding(int size) {
        if (size < 0) return 0;
        return (8 - (size % 8)) % 8;
    }

    /**
     * Aligns a size to an 8-byte boundary.
     *
     * @param size the size to align
     * @return the aligned size
     */
    private static int alignToEightBytes(int size) {
        return (size + 7) & ~7;
    }

    /**
     * Returns a string representation of the HdfGlobalHeap.
     *
     * @return a string describing the number of loaded heaps and their offsets
     */
    @Override
    public String toString() {
        return "HdfGlobalHeap{" + "loadedHeapCount=" + heapCollections.size() + ", knownOffsets=" + heapCollections.keySet() + '}';
    }

    /**
     * Interface for initializing global heap collections lazily.
     */
    public interface GlobalHeapInitialize {
        /**
         * Callback to initialize a heap collection at the specified offset.
         *
         * @param heapOffset the offset of the heap collection
         */
        void initializeCallback(long heapOffset);
    }

    /**
     * Represents a single object in a global heap collection.
     */
    private static class GlobalHeapObject {
        /** The unique ID of the object (0 for null terminator). */
        private final int heapObjectIndex;
        /** The reference count of the object. */
        private final int referenceCount;
        /** The size of the object data or free space (for null terminator). */
        private final long objectSize;
        /** The data bytes of the object (null for null terminator). */
        private final byte[] data;

        /**
         * Constructs a GlobalHeapObject.
         *
         * @param heapObjectIndex      the object ID
         * @param referenceCount the reference count
         * @param sizeOrFreeSpace the size of the data or free space
         * @param data          the data bytes (null for ID 0)
         * @throws IllegalArgumentException if data constraints are violated
         */
        private GlobalHeapObject(int heapObjectIndex, int referenceCount, long sizeOrFreeSpace, byte[] data) {
            this.heapObjectIndex = heapObjectIndex;
            this.referenceCount = referenceCount;
            this.objectSize = sizeOrFreeSpace;
            if (heapObjectIndex == 0) {
                this.data = null;
                if (data != null && data.length > 0) {
                    throw new IllegalArgumentException("Data must be null for Global Heap Object ID 0");
                }
            } else {
                if (data == null) {
                    throw new IllegalArgumentException("Data cannot be null for non-zero Global Heap Object ID: " + heapObjectIndex);
                }
                if (data.length != sizeOrFreeSpace) {
                    throw new IllegalArgumentException("Data length ("+data.length+") must match objectSize ("+sizeOrFreeSpace+") for non-zero objectId "+ heapObjectIndex);
                }
                this.data = data;
            }
        }

        /**
         * Reads a GlobalHeapObject from a ByteBuffer.
         *
         * @param buffer the ByteBuffer to read from
         * @return the constructed GlobalHeapObject
         * @throws RuntimeException if the buffer data is insufficient or invalid
         */
        public static GlobalHeapObject readFromByteBuffer(ByteBuffer buffer) {
            if (buffer.remaining() < 16) { throw new RuntimeException("Buffer underflow: insufficient data for Global Heap Object header (needs 16 bytes, found " + buffer.remaining() + ")"); }
            int objectId = Short.toUnsignedInt(buffer.getShort());
            int referenceCount = Short.toUnsignedInt(buffer.getShort());
            buffer.getInt();
            long sizeOrFreeSpace = buffer.getLong();
            if (objectId == 0) {
                if (sizeOrFreeSpace < 0) {
                    throw new RuntimeException("Invalid negative free space (" + sizeOrFreeSpace + ") indicated by null terminator object (ID 0).");
                }
                return new GlobalHeapObject(objectId, referenceCount, sizeOrFreeSpace, null);
            } else {
                if (sizeOrFreeSpace <= 0) {
                    throw new RuntimeException("Invalid non-positive object size (" + sizeOrFreeSpace + ") read for non-null object ID: " + objectId);
                }
                if (sizeOrFreeSpace > Integer.MAX_VALUE) {
                    throw new RuntimeException("Object size (" + sizeOrFreeSpace + ") exceeds maximum Java array size (Integer.MAX_VALUE) for object ID: " + objectId);
                }
                int actualObjectSize = (int) sizeOrFreeSpace;
                int padding = getPadding(actualObjectSize);
                int requiredBytes = actualObjectSize + padding;
                if (buffer.remaining() < requiredBytes) {
                    throw new RuntimeException("Buffer underflow: insufficient data for object content and padding (needs " + requiredBytes + " bytes [data:" + actualObjectSize + ", pad:" + padding + "], found " + buffer.remaining() + ") for object ID: " + objectId);
                }
                byte[] objectData = new byte[actualObjectSize];
                buffer.get(objectData);
                if (padding > 0) {
                    buffer.position(buffer.position() + padding);
                }
                return new GlobalHeapObject(objectId, referenceCount, sizeOrFreeSpace, objectData);
            }
        }

        /**
         * Writes the GlobalHeapObject to a ByteBuffer.
         *
         * @param buffer the ByteBuffer to write to
         * @throws IllegalStateException if the object data is inconsistent
         */
        public void writeToByteBuffer(ByteBuffer buffer) {
            buffer.putShort((short) heapObjectIndex);
            buffer.putShort((short) referenceCount);
            buffer.putInt(0);
            buffer.putLong(objectSize);
            if (heapObjectIndex != 0) {
                int expectedDataSize = (int)objectSize;
                if (data == null || data.length != expectedDataSize) {
                    throw new IllegalStateException("Object data is inconsistent or null for writing object ID: " + heapObjectIndex + ". Expected size " + expectedDataSize + ", data length " + (data != null ? data.length : "null"));
                }
                buffer.put(data);
                int padding = getPadding(expectedDataSize);
                if (padding > 0) { buffer.put(new byte[padding]); }
            }
        }

        public int getHeapObjectIndex() {
            return heapObjectIndex;
        }

        public long getObjectSize() {
            return objectSize;
        }

        public byte[] getData() {
            return data;
        }
    }
}