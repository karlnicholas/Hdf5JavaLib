package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.AllocationType;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;

public class HdfGlobalHeapBlock {
    static final int GLOBAL_HEAP_OBJECT_SIZE = 16;
    private final LinkedHashMap<Integer, GlobalHeapObject> globalHeapObjects;
    /**
     * Map of heap offsets to the next available object ID.
     */
    private Integer nextObjectId;
    private final HdfDataFile hdfDataFile;
    private HdfFixedPoint offset;
    /**
     * Constructs an allocation record.
     *
     * @param type           the allocation type
     * @param name           the name of the allocation
     * @param offset         the starting offset
     */
    public HdfGlobalHeapBlock(
            LinkedHashMap<Integer, GlobalHeapObject> globalHeapObjects,
            HdfFixedPoint collectionSize,
            Integer nextObjectId,
            HdfDataFile hdfDataFile,
            AllocationType type,
            String name,
            HdfFixedPoint offset
    ) {
        this.hdfDataFile = hdfDataFile;
        this.globalHeapObjects = globalHeapObjects;
        this.nextObjectId = nextObjectId;
        this.offset = offset;
    }

    public GlobalHeapObject getGlobalHeapObject(int objectId) {
        return globalHeapObjects.get(objectId);
    }

    public byte[] getDataBytes(HdfFixedPoint heapOffset, int objectId) {
        GlobalHeapObject obj = getGlobalHeapObject(objectId);
        //.getGlobalHeapObject(objectId);
        if (obj == null) {
            throw new IllegalStateException("No object found for objectId: " + objectId + " in heap at offset: " + heapOffset);
        }
        if (obj.getHeapObjectIndex() == 0) {
            throw new IllegalStateException("Internal error: Object ID 0 found unexpectedly during data retrieval for offset: " + heapOffset);
        }
        return obj.getData();
    }

//    /**
//     * Writes all global heap collections to a file channel.
//     *
//     * @param fileChannel the file channel to write to
//     * @throws IOException if an I/O error occurs
//     */
//    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
//        if (heapCollections.isEmpty()) {
//            return;
//        }
//
//        for (Map.Entry<HdfFixedPoint, LinkedHashMap<Integer, GlobalHeapObject>> entry : heapCollections.entrySet()) {
//            HdfFixedPoint heapOffset = entry.getKey();
//            LinkedHashMap<Integer, GlobalHeapObject> objects = entry.getValue();
//
//            HdfFixedPoint heapSize = this.hdfDataFile.getFileAllocation().getGlobalHeapBlockSize(heapOffset);
//            fileChannel.position(heapOffset.getInstance(Long.class));
//            int size1 = getWriteBufferSize(heapOffset).getInstance(Long.class).intValue();
//            ByteBuffer buffer = ByteBuffer.allocate(heapSize.getInstance(Integer.class));
//            buffer.order(ByteOrder.LITTLE_ENDIAN);
//
//            buffer.put(GLOBAL_HEAP_SIGNATURE);
//            buffer.put((byte) VERSION);
//            buffer.put(new byte[3]);
//            buffer.putLong(calculateAlignedTotalSize(heapOffset).getInstance(Long.class));
//
//            for (GlobalHeapObject obj : objects.values()) {
//                obj.writeToByteBuffer(buffer);
//            }
//
//            if (!objects.containsKey(0)) {
//                long usedSize = GLOBAL_HEAP_OBJECT_SIZE;
//                for (GlobalHeapObject obj : objects.values()) {
//                    long objSize = obj.getHeapObjectIndex() == 0 ? GLOBAL_HEAP_OBJECT_SIZE : GLOBAL_HEAP_OBJECT_SIZE + obj.getObjectSize() + getPadding((int) obj.getObjectSize());
//                    usedSize += objSize;
//                }
//                long blockSize = hdfDataFile.getFileAllocation().getGlobalHeapBlockSize(heapOffset).getInstance(Long.class);
//                long remainingSize = blockSize - usedSize;
//                if (remainingSize < GLOBAL_HEAP_OBJECT_SIZE) {
//                    throw new IllegalStateException("Insufficient space for null terminator in heap at offset: " + heapOffset);
//                }
//                buffer.putShort((short) 0);
//                buffer.putShort((short) 0);
//                buffer.putInt(0);
//                buffer.putLong(remainingSize);
//            }
//
//            buffer.rewind();
//            fileChannel.write(buffer);
//        }
//    }

    /**
     * Calculates the aligned total size of a heap collection.
     *
     * @return the aligned total size in bytes
     * @throws IllegalStateException if the heap collection is not found
     */
    private HdfFixedPoint calculateAlignedTotalSize() {
//        LinkedHashMap<Integer, GlobalHeapObject> objects = heapCollections.get(heapOffset);
//        if (objects == null) {
//            throw new IllegalStateException("No heap collection found at offset: " + heapOffset);
//        }
//
        long totalSize = GLOBAL_HEAP_OBJECT_SIZE;
        for (GlobalHeapObject obj : globalHeapObjects.values()) {
            long objSize = obj.getHeapObjectIndex() == 0 ? GLOBAL_HEAP_OBJECT_SIZE : GLOBAL_HEAP_OBJECT_SIZE + obj.getObjectSize() + getPadding((int) obj.getObjectSize());
            totalSize += objSize;
        }

        if (!globalHeapObjects.containsKey(0)) {
            totalSize += GLOBAL_HEAP_OBJECT_SIZE;
        }

//        HdfFixedPoint blockSize = hdfDataFile.getFileAllocation().getGlobalHeapBlockSize(allocationRecord.getOffset());
//        long aligned = alignTo(totalSize, blockSize.getInstance(Long.class));
        long aligned = alignTo(totalSize, 4096);
        return HdfWriteUtils.hdfFixedPointFromValue(aligned, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset());
    }

    /**
     * Returns the size of the write buffer needed for a heap collection.
     *
     * @return the buffer size in bytes
     */
    public HdfFixedPoint getWriteBufferSize() {
        return calculateAlignedTotalSize();
    }

    /**
     * Aligns a size to the specified alignment boundary.
     *
     * @param size      the size to align
     * @param alignment the alignment boundary (must be a power of 2)
     * @return the aligned size
     * @throws IllegalArgumentException if the alignment is not a positive power of 2
     */
    private static long alignTo(long size, long alignment) {
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
        return "HdfGlobalHeap{" + "loadedHeapCount=" + globalHeapObjects.size() + ", knownOffsets=" + globalHeapObjects.keySet() + '}';
    }

    public boolean attemptAddBytes(byte[] bytes) {
        long currentUsedSize = GLOBAL_HEAP_OBJECT_SIZE;
        for (GlobalHeapObject existingObj : globalHeapObjects.values()) {
            currentUsedSize += GLOBAL_HEAP_OBJECT_SIZE;
            long existingObjectSize = existingObj.getObjectSize();
            if (existingObjectSize > Integer.MAX_VALUE) {
                throw new IllegalStateException("Existing object " + existingObj.getHeapObjectIndex() + " size too large to calculate padding.");
            }
            currentUsedSize += existingObjectSize;
            currentUsedSize += getPadding((int) existingObjectSize);
        }

        int newObjectDataSize = bytes.length;
        int newObjectPadding = getPadding(newObjectDataSize);
        long newObjectRequiredSize = GLOBAL_HEAP_OBJECT_SIZE + (long)(newObjectDataSize + newObjectPadding);

//        HdfFixedPoint blockSize = allocationRecord.getSize();
        // need a new heap
//        boolean canAcceptBytes = currentUsedSize + newObjectRequiredSize + GLOBAL_HEAP_OBJECT_SIZE > blockSize.getInstance(Long.class);
        boolean canAcceptBytes = currentUsedSize + newObjectRequiredSize + GLOBAL_HEAP_OBJECT_SIZE > 4096;
        if ( !canAcceptBytes ) {
            // Add null terminator to mark the block as full
            long freeSpace = 4096 - currentUsedSize;
            if (freeSpace < GLOBAL_HEAP_OBJECT_SIZE) {
                throw new IllegalStateException("Insufficient space for null terminator in heap at offset " + offset);
            }
            if (!globalHeapObjects.containsKey(0)) {
                GlobalHeapObject nullTerminator = new GlobalHeapObject(0, 0, freeSpace, null);
                globalHeapObjects.put(0, nullTerminator);
            }

        }
        return canAcceptBytes;
    }

    public byte[] addToHeap(byte[] bytes) {
//        long currentUsedSize = GLOBAL_HEAP_OBJECT_SIZE;
//        for (GlobalHeapObject existingObj : globalHeapObjects.values()) {
//            currentUsedSize += GLOBAL_HEAP_OBJECT_SIZE;
//            long existingObjectSize = existingObj.getObjectSize();
//            if (existingObjectSize > Integer.MAX_VALUE) {
//                throw new IllegalStateException("Existing object " + existingObj.getHeapObjectIndex() + " size too large to calculate padding.");
//            }
//            currentUsedSize += existingObjectSize;
//            currentUsedSize += getPadding((int) existingObjectSize);
//        }
//
        int newObjectDataSize = bytes.length;
//        int newObjectPadding = getPadding(newObjectDataSize);
//        long newObjectRequiredSize = GLOBAL_HEAP_OBJECT_SIZE + newObjectDataSize + newObjectPadding;
//
//        HdfFixedPoint blockSize = this.getSize();
        // need a new heap
//        } else if (targetObjects.containsKey(0)) {
//            // Remove null terminator to allow new object insertion
//            targetObjects.remove(0);
//        }

//        int currentNextId = nextObjectIds.getOrDefault(currentHeapOffset, 1);
//        if (currentNextId > 0xFFFF) {
//            throw new IllegalStateException("Maximum number of global heap objects (65535) exceeded for heap at offset " + currentHeapOffset);
//        }

        GlobalHeapObject obj = new GlobalHeapObject(nextObjectId, 0, newObjectDataSize, bytes);
        globalHeapObjects.put(nextObjectId, obj);
        nextObjectId = nextObjectId + 1;

        ByteBuffer buffer = ByteBuffer.allocate(GLOBAL_HEAP_OBJECT_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(newObjectDataSize);
//        buffer.putLong(allocationRecord.getOffset().getInstance(Long.class));
        buffer.putLong(0);
        buffer.putInt(nextObjectId);
        return buffer.array();
    }

    /**
     * Represents a single object in a global heap collection.
     */
    static class GlobalHeapObject {
        /**
         * The unique ID of the object (0 for null terminator).
         */
        private final int heapObjectIndex;
        /**
         * The reference count of the object.
         */
        private final int referenceCount;
        /**
         * The size of the object data or free space (for null terminator).
         */
        private final long objectSize;
        /**
         * The data bytes of the object (null for null terminator).
         */
        private final byte[] data;

        /**
         * Constructs a GlobalHeapObject.
         *
         * @param heapObjectIndex the object ID
         * @param referenceCount  the reference count
         * @param sizeOrFreeSpace the size of the data or free space
         * @param data            the data bytes (null for ID 0)
         * @throws IllegalArgumentException if data constraints are violated
         */
        GlobalHeapObject(int heapObjectIndex, int referenceCount, long sizeOrFreeSpace, byte[] data) {
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
                    throw new IllegalArgumentException("Data length (" + data.length + ") must match objectSize (" + sizeOrFreeSpace + ") for non-zero objectId " + heapObjectIndex);
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
            if (buffer.remaining() < GLOBAL_HEAP_OBJECT_SIZE) {
                throw new IllegalStateException("Buffer underflow: insufficient data for Global Heap Object header (needs 16 bytes, found " + buffer.remaining() + ")");
            }
            int objectId = Short.toUnsignedInt(buffer.getShort());
            int referenceCount = Short.toUnsignedInt(buffer.getShort());
            buffer.getInt();
            long sizeOrFreeSpace = buffer.getLong();
            if (objectId == 0) {
                if (sizeOrFreeSpace < 0) {
                    throw new IllegalStateException("Invalid negative free space (" + sizeOrFreeSpace + ") indicated by null terminator object (ID 0).");
                }
                return new GlobalHeapObject(objectId, referenceCount, sizeOrFreeSpace, null);
            } else {
                if (sizeOrFreeSpace <= 0) {
                    throw new IllegalStateException("Invalid non-positive object size (" + sizeOrFreeSpace + ") read for non-null object ID: " + objectId);
                }
                if (sizeOrFreeSpace > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Object size (" + sizeOrFreeSpace + ") exceeds maximum Java array size (Integer.MAX_VALUE) for object ID: " + objectId);
                }
                int actualObjectSize = (int) sizeOrFreeSpace;
                int padding = getPadding(actualObjectSize);
                int requiredBytes = actualObjectSize + padding;
                if (buffer.remaining() < requiredBytes) {
                    throw new IllegalStateException("Buffer underflow: insufficient data for object content and padding (needs " + requiredBytes + " bytes [data:" + actualObjectSize + ", pad:" + padding + "], found " + buffer.remaining() + ") for object ID: " + objectId);
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
                int expectedDataSize = (int) objectSize;
                if (data == null || data.length != expectedDataSize) {
                    throw new IllegalStateException("Object data is inconsistent or null for writing object ID: " + heapObjectIndex + ". Expected size " + expectedDataSize + ", data length " + (data != null ? data.length : "null"));
                }
                buffer.put(data);
                int padding = getPadding(expectedDataSize);
                if (padding > 0) {
                    buffer.put(new byte[padding]);
                }
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
