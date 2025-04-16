package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfFileAllocation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@Slf4j
public class HdfGlobalHeap {
    private static final String SIGNATURE = "GCOL";
    private static final int VERSION = 1;

    private final Map<Long, TreeMap<Integer, GlobalHeapObject>> heapCollections;
    private final Map<Long, HdfFixedPoint> collectionSizes;
    private final Map<Long, Integer> nextObjectIds;
    private long currentWriteHeapOffset = -1L;
    private final GlobalHeapInitialize initialize;
    private final HdfDataFile dataFile;

    public HdfGlobalHeap(GlobalHeapInitialize initialize, HdfDataFile dataFile) {
        this.initialize = initialize;
        this.dataFile = dataFile;
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
        this.currentWriteHeapOffset = -1L;
    }

    public HdfGlobalHeap(HdfDataFile dataFile) {
        this.dataFile = dataFile;
        this.initialize = null;
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
        this.currentWriteHeapOffset = -1L;
    }

    public byte[] getDataBytes(long heapOffset, int objectId) {
        if (objectId == 0) {
            throw new IllegalArgumentException("Cannot request data bytes for Global Heap Object ID 0 (null terminator)");
        }
        TreeMap<Integer, GlobalHeapObject> specificHeapObjects = heapCollections.get(heapOffset);
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
        if (obj.getObjectId() == 0) {
            throw new RuntimeException("Internal error: Object ID 0 found unexpectedly during data retrieval for offset: " + heapOffset);
        }
        return obj.getData();
    }

    public void readFromFileChannel(SeekableByteChannel fileChannel, short ignoredOffsetSize) throws IOException {
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
        HdfFixedPoint localCollectionSize = HdfFixedPoint.readFromByteBuffer(headerBuffer, (short) 8, new BitSet(), (short) 0, (short) 64);
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

        TreeMap<Integer, GlobalHeapObject> localObjects = new TreeMap<>();
        int localNextObjectId = 1;

        try {
            if (objectBuffer != null) {
                while (objectBuffer.hasRemaining()) {
                    if (objectBuffer.remaining() < 16) {
                        break;
                    }
                    GlobalHeapObject obj = GlobalHeapObject.readFromByteBuffer(objectBuffer);
                    if (obj.getObjectId() == 0) {
                        break;
                    }
                    if (localObjects.containsKey(obj.getObjectId())) {
                        throw new RuntimeException("Duplicate object ID " + obj.getObjectId() + " found in heap at offset: " + startOffset);
                    }
                    if (obj.getObjectId() < 1 || obj.getObjectId() > 0xFFFF) {
                        throw new RuntimeException("Invalid object ID " + obj.getObjectId() + " found in heap at offset: " + startOffset);
                    }
                    localObjects.put(obj.getObjectId(), obj);
                    localNextObjectId = Math.max(localNextObjectId, obj.getObjectId() + 1);
                }
            }
        } catch (Exception e) {
            throw new IOException("Unexpected error processing global heap object data buffer at offset: " + startOffset, e);
        }

        this.heapCollections.put(startOffset, localObjects);
        this.collectionSizes.put(startOffset, localCollectionSize);
        this.nextObjectIds.put(startOffset, localNextObjectId);
    }

    public byte[] addToHeap(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Input byte array cannot be null.");
        }

        HdfFileAllocation fileAllocation = dataFile.getFileAllocation();

        if (this.currentWriteHeapOffset == -1L) {
            this.currentWriteHeapOffset = fileAllocation.getGlobalHeapOffset();
            if (this.currentWriteHeapOffset == -1L) {
                throw new IllegalStateException("The first Global Heap block has not been allocated yet. Call HdfFileAllocation.allocateFirstGlobalHeapBlock() first.");
            }
        }
        long currentHeapOffset = this.currentWriteHeapOffset;

        TreeMap<Integer, GlobalHeapObject> targetObjects = heapCollections.computeIfAbsent(currentHeapOffset, k -> new TreeMap<>());

        long currentUsedSize = 16;
        for (GlobalHeapObject existingObj : targetObjects.values()) {
            if (existingObj.getObjectId() == 0) continue;
            currentUsedSize += 16;
            long existingObjectSize = existingObj.getObjectSize();
            if (existingObjectSize > Integer.MAX_VALUE) {
                throw new IllegalStateException("Existing object " + existingObj.getObjectId() + " size too large to calculate padding.");
            }
            currentUsedSize += existingObjectSize;
            currentUsedSize += getPadding((int) existingObjectSize);
        }

        int newObjectDataSize = bytes.length;
        int newObjectPadding = getPadding(newObjectDataSize);
        long newObjectRequiredSize = 16L + newObjectDataSize + newObjectPadding;

        long blockSize = fileAllocation.getGlobalHeapBlockSize(currentHeapOffset);
        if (currentUsedSize + newObjectRequiredSize + 16L > blockSize) {
            // Add null terminator only for first block transition
            if (currentHeapOffset == fileAllocation.getGlobalHeapOffset()) {
                currentUsedSize += 16L;
                long freeSpace = blockSize - currentUsedSize;
                if (freeSpace < 0) {
                    throw new IllegalStateException("Internal error: Calculated negative free space (" + freeSpace + ") for heap at offset " + currentHeapOffset);
                }
                GlobalHeapObject nullTerminator = new GlobalHeapObject(0, 0, freeSpace, null);
                targetObjects.put(0, nullTerminator);
            } else {
                // Remove existing null terminator for second block expansion
                targetObjects.remove(0);
            }

            long newHeapOffset;
            if (currentHeapOffset == fileAllocation.getGlobalHeapOffset()) {
                newHeapOffset = fileAllocation.allocateNextGlobalHeapBlock();
            } else {
                newHeapOffset = fileAllocation.expandGlobalHeapBlock();
            }
            this.currentWriteHeapOffset = newHeapOffset;
            currentHeapOffset = newHeapOffset;
            targetObjects = heapCollections.computeIfAbsent(currentHeapOffset, k -> new TreeMap<>());
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

    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
        if (heapCollections.isEmpty()) {
            return;
        }

        for (Map.Entry<Long, TreeMap<Integer, GlobalHeapObject>> entry : heapCollections.entrySet()) {
            long heapOffset = entry.getKey();
            TreeMap<Integer, GlobalHeapObject> objects = entry.getValue();

            fileChannel.position(heapOffset);
            ByteBuffer buffer = ByteBuffer.allocate((int) getWriteBufferSize(heapOffset));
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
                    long objSize = obj.getObjectId() == 0 ? 16 : 16 + obj.getObjectSize() + getPadding((int) obj.getObjectSize());
                    usedSize += objSize;
                }
                long blockSize = dataFile.getFileAllocation().getGlobalHeapBlockSize(heapOffset);
                long remainingSize = blockSize - usedSize;
                if (remainingSize < 16) {
                    throw new IllegalStateException("Insufficient space for null terminator in heap at offset: " + heapOffset);
                }
                buffer.putShort((short) 0);
                buffer.putShort((short) 0);
                buffer.putInt(0);
                buffer.putLong(remainingSize);
            }

            buffer.flip();
            fileChannel.write(buffer);
        }
    }

    private long calculateAlignedTotalSize(long heapOffset) {
        TreeMap<Integer, GlobalHeapObject> objects = heapCollections.get(heapOffset);
        if (objects == null) {
            throw new IllegalStateException("No heap collection found at offset: " + heapOffset);
        }

        long totalSize = 16;
        for (GlobalHeapObject obj : objects.values()) {
            long objSize = obj.getObjectId() == 0 ? 16 : 16 + obj.getObjectSize() + getPadding((int) obj.getObjectSize());
            totalSize += objSize;
        }

        if (!objects.containsKey(0)) {
            totalSize += 16;
        }

        long blockSize = dataFile.getFileAllocation().getGlobalHeapBlockSize(heapOffset);
        return alignTo(totalSize, (int) blockSize);
    }

    public long getWriteBufferSize(long heapOffset) {
        return calculateAlignedTotalSize(heapOffset);
    }

    private static long alignTo(long size, int alignment) {
        if (alignment <= 0 || (alignment & (alignment - 1)) != 0) {
            throw new IllegalArgumentException("Alignment must be a positive power of 2. Got: " + alignment);
        }
        return (size + alignment - 1) & ~((long) alignment - 1);
    }

    private static int getPadding(int size) {
        if (size < 0) return 0;
        return (8 - (size % 8)) % 8;
    }

    private static int alignToEightBytes(int size) {
        return (size + 7) & ~7;
    }

    @Override
    public String toString() {
        return "HdfGlobalHeap{" + "loadedHeapCount=" + heapCollections.size() + ", knownOffsets=" + heapCollections.keySet() + '}';
    }

    public interface GlobalHeapInitialize {
        void initializeCallback(long heapOffset);
    }

    @Getter
    private static class GlobalHeapObject {
        private final int objectId;
        private final int referenceCount;
        private final long objectSize;
        private final byte[] data;

        private GlobalHeapObject(int objectId, int referenceCount, long sizeOrFreeSpace, byte[] data) {
            this.objectId = objectId;
            this.referenceCount = referenceCount;
            this.objectSize = sizeOrFreeSpace;
            if (objectId == 0) {
                this.data = null;
                if (data != null && data.length > 0) {
                    throw new IllegalArgumentException("Data must be null for Global Heap Object ID 0");
                }
            } else {
                if (data == null) {
                    throw new IllegalArgumentException("Data cannot be null for non-zero Global Heap Object ID: " + objectId);
                }
                if (data.length != sizeOrFreeSpace) {
                    throw new IllegalArgumentException("Data length ("+data.length+") must match objectSize ("+sizeOrFreeSpace+") for non-zero objectId "+objectId);
                }
                this.data = data;
            }
        }

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

        public void writeToByteBuffer(ByteBuffer buffer) {
            buffer.putShort((short) objectId);
            buffer.putShort((short) referenceCount);
            buffer.putInt(0);
            buffer.putLong(objectSize);
            if (objectId != 0) {
                int expectedDataSize = (int)objectSize;
                if (data == null || data.length != expectedDataSize) {
                    throw new IllegalStateException("Object data is inconsistent or null for writing object ID: " + objectId + ". Expected size " + expectedDataSize + ", data length " + (data != null ? data.length : "null"));
                }
                buffer.put(data);
                int padding = getPadding(expectedDataSize);
                if (padding > 0) { buffer.put(new byte[padding]); }
            }
        }
    }
}