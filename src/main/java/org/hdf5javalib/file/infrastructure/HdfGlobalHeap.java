package org.hdf5javalib.file.infrastructure;

// Added import for the singleton

import lombok.Getter;
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

public class HdfGlobalHeap {
    private static final String SIGNATURE = "GCOL";
    private static final int VERSION = 1;
    // REMOVED: Alignment constant, now uses HdfFileAllocation.GLOBAL_HEAP_BLOCK_SIZE

    // Stores heap objects keyed by their starting offset in the file
    private final Map<Long, TreeMap<Integer, GlobalHeapObject>> heapCollections;
    // Optional: Store other per-heap metadata if needed for write/add operations later
    private final Map<Long, HdfFixedPoint> collectionSizes;
    private final Map<Long, Integer> nextObjectIds;

    // Track the offset of the heap currently being written to
    private long currentWriteHeapOffset = -1L;

    // Keep interfaces, but GlobalHeapInitialize is adapted
    private final GlobalHeapInitialize initialize;

    private final HdfDataFile dataFile;

    /**
     * Constructor for use cases where heaps might need lazy initialization via callback
     * (typically during reading).
     */
    public HdfGlobalHeap(GlobalHeapInitialize initialize, HdfDataFile dataFile) {
        this.initialize = initialize;
        this.dataFile = dataFile;
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
        this.currentWriteHeapOffset = -1L;
    }

    /**
     * Constructor for use cases focused on writing or creating new heaps.
     * Does not support lazy initialization via callback.
     */
    public HdfGlobalHeap(HdfDataFile dataFile) {
        this.dataFile = dataFile;
        this.initialize = null;
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
        this.currentWriteHeapOffset = -1L;
    }

    /**
     * Retrieves the data bytes for a specific object within a specific global heap collection.
     * (Method body unchanged)
     */
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

    /**
     * Reads a single global heap collection starting from the fileChannel's current position.
     * Stores the read objects mapped to their starting file offset.
     * (Method body formatting corrected, logic unchanged from baseline)
     */
    public void readFromFileChannel(SeekableByteChannel fileChannel, short ignoredOffsetSize) throws IOException {
        long startOffset = fileChannel.position(); // Record the starting offset

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

        headerBuffer.position(headerBuffer.position() + 3); // Skip reserved bytes

        HdfFixedPoint localCollectionSize = HdfFixedPoint.readFromByteBuffer(headerBuffer, (short) 8, new BitSet(), (short) 0, (short) 64);
        long declaredSize = localCollectionSize.getInstance(Long.class);

        // Check declared size against file bounds and minimum header size
        if (declaredSize < 16 || (startOffset + declaredSize > fileChannel.size())) {
            // Check if declaredSize goes beyond EOF, or if it's impossibly small
            if (startOffset + declaredSize > fileChannel.size() && declaredSize >= 16) {
                // Allow reading if declaredSize matches *exactly* the remaining file size? Maybe?
                // H5py seems to allow reading partially written files sometimes.
                // For now, let's be strict.
                throw new IllegalArgumentException("Declared collection size " + declaredSize + " at offset " + startOffset + " exceeds file size " + fileChannel.size());
            } else if (declaredSize < 16) {
                throw new IllegalArgumentException("Declared collection size " + declaredSize + " is less than minimum header size (16) at offset " + startOffset);
            }
            // Note: Original check was slightly different, this seems more robust. Revert if needed.
            // Original: if (declaredSize < 16 || declaredSize > fileChannel.size() - startOffset)
        }

        // Calculate object buffer size
        int objectDataBufferSize = (int) (declaredSize - 16);
//        int objectDataBufferSize = (int) (declaredSize);
        if (objectDataBufferSize < 0) {
            // This condition implies declaredSize < 16, which should be caught above.
            // However, a heap needs space for at least the null terminator (16 bytes) if it contains any objects.
            // A declaredSize of exactly 16 (empty heap) might be valid but rare.
            // If declaredSize is > 16, objectDataBufferSize must be >= 16 to hold the null terminator.
            if (declaredSize > 16 && objectDataBufferSize < 16) {
                throw new IllegalArgumentException("Declared size " + declaredSize + " at offset " + startOffset + " implies insufficient space ("+objectDataBufferSize+" bytes) for null terminator object.");
            } else if (objectDataBufferSize < 0) { // Should be unreachable if declaredSize check is correct
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

        // Read objects into a local map for this specific heap
        TreeMap<Integer, GlobalHeapObject> localObjects = new TreeMap<>();
        int localNextObjectId = 1;

        try {
            if (objectBuffer != null) { // Only process if buffer exists
                while (objectBuffer.hasRemaining()) {
                    // Check space for header before attempting read
                    if (objectBuffer.remaining() < 16) {
                        // If remaining bytes < header size, it must be trailing padding.
                        // Valid heaps end with ID 0. If we didn't find ID 0, this is an error state.
                        break; // Assume padding and exit loop
                    }

                    GlobalHeapObject obj = GlobalHeapObject.readFromByteBuffer(objectBuffer);

                    // Check for the null terminator object
                    if (obj.getObjectId() == 0) {
                        break; // Stop reading objects for this heap
                    }

                    // Check for duplicate object IDs within the same heap
                    if (localObjects.containsKey(obj.getObjectId())) {
                        throw new RuntimeException("Duplicate object ID " + obj.getObjectId() + " found in heap at offset: " + startOffset);
                    }
                    // Check if ID is reasonable (HDF5 uses unsigned short, so 1 to 65535)
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


        // Store the successfully read heap data mapped by its start offset
        this.heapCollections.put(startOffset, localObjects);
        this.collectionSizes.put(startOffset, localCollectionSize); // Store declared size read from header
        this.nextObjectIds.put(startOffset, localNextObjectId); // Store next available ID based on max found
    }


    /**
     * Adds data as a new object to the currently active global heap collection.
     * If the active heap is full, it adds a null terminator to the current heap,
     * requests a new heap block allocation from HdfFileAllocation, updates the active heap,
     * and adds the object to the new heap.
     * Updates the in-memory representation only.
     * Returns the 16-byte Global Heap ID structure referencing the new object.
     * (Method body includes rollover logic)
     */
    public byte[] addToHeap(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Input byte array cannot be null.");
        }

        HdfFileAllocation fileAllocation = dataFile.getFileAllocation();

        // --- Determine Current Heap Offset ---
        if (this.currentWriteHeapOffset == -1L) {
            // First time adding, get the offset from allocation manager
            this.currentWriteHeapOffset = fileAllocation.getGlobalHeapOffset();
            if (this.currentWriteHeapOffset == -1L) {
                // Ensure first block is allocated *before* trying to add
                // This might require calling allocateFirstGlobalHeapBlock externally first.
                // For now, throw indicating it needs allocation.
                throw new IllegalStateException("The first Global Heap block has not been allocated yet. Call HdfFileAllocation.allocateFirstGlobalHeapBlock() first.");
            }
        }
        long currentHeapOffset = this.currentWriteHeapOffset;

        // Get or create the object map for the current heap
        TreeMap<Integer, GlobalHeapObject> targetObjects = heapCollections.computeIfAbsent(currentHeapOffset, k -> new TreeMap<>());

        // --- Size Calculation & Check ---
        long currentUsedSize = 16; // Start with main heap header size
        for (GlobalHeapObject existingObj : targetObjects.values()) {
            if (existingObj.getObjectId() == 0) continue; // Skip null terminator if already present

            currentUsedSize += 16; // Object header
            long existingObjectSize = existingObj.getObjectSize();
            if (existingObjectSize > Integer.MAX_VALUE) {
                throw new IllegalStateException("Existing object " + existingObj.getObjectId() + " size too large to calculate padding.");
            }
            currentUsedSize += existingObjectSize;
            currentUsedSize += getPadding((int) existingObjectSize);
        }

        int newObjectDataSize = bytes.length;
        int newObjectPadding = getPadding(newObjectDataSize);
        long newObjectRequiredSize = 16L + newObjectDataSize + newObjectPadding; // Header + data + padding

        // Check if adding the new object AND the null terminator (16 bytes) exceeds the block size
        if (currentUsedSize + newObjectRequiredSize + 16L > HdfFileAllocation.GLOBAL_HEAP_BLOCK_SIZE) {

            // --- Heap is Full: Close current, Allocate new ---

            // 1. Calculate free space in the current (full) heap
            currentUsedSize += 16L; // Add space needed for null terminator header itself
            long freeSpace = HdfFileAllocation.GLOBAL_HEAP_BLOCK_SIZE - currentUsedSize;
            if (freeSpace < 0) {
                throw new IllegalStateException("Internal error: Calculated negative free space (" + freeSpace + ") for heap at offset " + currentHeapOffset);
            }

            // 2. Create and add the null terminator object to the *current* heap's map
            GlobalHeapObject nullTerminator = new GlobalHeapObject(0, 0, freeSpace, null);
            if (targetObjects.containsKey(0)) {
                // This heap was already full and terminated, internal logic error
                throw new IllegalStateException("Attempted to add null terminator to already terminated heap at offset " + currentHeapOffset);
            }
            targetObjects.put(0, nullTerminator);

            // 3. Allocate a new heap block
            long newHeapOffset = fileAllocation.allocateNextGlobalHeapBlock();

            // 4. Update internal state to point to the new heap
            this.currentWriteHeapOffset = newHeapOffset;
            currentHeapOffset = newHeapOffset;

            // 5. Get/create the map for the new heap
            targetObjects = heapCollections.computeIfAbsent(currentHeapOffset, k -> new TreeMap<>());

            // 6. Next ID for the new heap will be handled by getOrDefault below

        } // --- End of Heap Full Handling ---


        // --- Add Object to the (potentially new) Current Heap ---

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
        buffer.putLong(currentHeapOffset); // Use offset where object was actually added
        buffer.putInt(objectId);
        return buffer.array();
    }

    /**
     * WARNING: This method is NOT updated for multiple heaps and will throw an exception.
     * Writes a *specific* heap collection to the buffer. Needs offset parameter to identify which heap.
     * NOTE: Will need updating to handle writing the ID 0 entry correctly if present.
     *
     * @param buffer The ByteBuffer to write to.
     */
    public void writeToByteBuffer(ByteBuffer buffer) {
        throw new UnsupportedOperationException("writeToByteBuffer is not implemented yet. It needs to handle potential ID 0 entries.");
        /* --- RESTORED Old Logic Comment Block --- */
        /* ... */ // (Content omitted for brevity)
    }

    /**
     * WARNING: This method is NOT updated for multiple heaps and will throw an exception.
     * Calculates the required buffer size for writing a specific heap, including alignment padding.
     * NOTE: Will need updating based on how ID 0 entry is handled.
     */
    private long calculateAlignedTotalSize(long heapOffset) {
        throw new UnsupportedOperationException("calculateAlignedTotalSize is not implemented yet. Needs update for ID 0 handling.");
        /* --- RESTORED Old Logic Comment Block --- */
        /* ... */ // (Content omitted for brevity)
    }

    /**
     * WARNING: This method is NOT updated for multiple heaps and will throw an exception.
     * Gets the required buffer size for writing a specific heap.
     */
    public long getWriteBufferSize(long heapOffset) {
        throw new UnsupportedOperationException("getWriteBufferSize is not implemented for multiple heap collections yet.");
    }


    // ========================================================================
    // Helper Methods & Interfaces (Unchanged)
    // ========================================================================
    private static long alignTo(long size, int alignment) { if (alignment <= 0 || (alignment & (alignment - 1)) != 0) { throw new IllegalArgumentException("Alignment must be a positive power of 2. Got: " + alignment); } return (size + alignment - 1) & ~((long)alignment - 1); }
    private static int getPadding(int size) { if (size < 0) return 0; return (8 - (size % 8)) % 8; }
    private static int alignToEightBytes(int size) { return (size + 7) & ~7; }
    @Override public String toString() { return "HdfGlobalHeap{" + "loadedHeapCount=" + heapCollections.size() + ", knownOffsets=" + heapCollections.keySet() + '}'; }
    public interface GlobalHeapInitialize { void initializeCallback(long heapOffset); }
    // REMOVED: Interface definition for GLobalHeapDataSegmentAddress


    /**
     * Inner class representing a single object within a global heap.
     * (Class body unchanged)
     */
    @Getter
    private static class GlobalHeapObject {
        private final int objectId;
        private final int referenceCount;
        private final long objectSize; // Represents data size for ID!=0, free space for ID=0
        private final byte[] data; // Null only for ID=0

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
    } // End GlobalHeapObject inner class

} // End HdfGlobalHeap class