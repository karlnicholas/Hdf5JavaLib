package org.hdf5javalib.file.infrastructure;

import lombok.Getter; // Assuming used by other parts of your project
import org.hdf5javalib.dataclass.HdfFixedPoint; // Assuming this class is available

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.BitSet; // Assuming needed by HdfFixedPoint
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class HdfGlobalHeap {
    private static final String SIGNATURE = "GCOL";
    private static final int VERSION = 1;
    private static final int ALIGNMENT = 4096; // Assuming this is relevant for write/size calculations

    // Stores heap objects keyed by their starting offset in the file
    private final Map<Long, TreeMap<Integer, GlobalHeapObject>> heapCollections;
    // Optional: Store other per-heap metadata if needed for write/add operations later
    private final Map<Long, HdfFixedPoint> collectionSizes;
    private final Map<Long, Integer> nextObjectIds;


    // Keep interfaces, but GlobalHeapInitialize is adapted
    private final GlobalHeapInitialize initialize;
    private final GLobalHeapDataSegmentAddress dataSegmentAddress; // Likely needed for writing later

    /**
     * Constructor for use cases where heaps might need lazy initialization via callback.
     */
    public HdfGlobalHeap(GlobalHeapInitialize initialize) {
        this.initialize = initialize;
        this.dataSegmentAddress = null; // Writing not supported via this constructor path initially
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
    }

    /**
     * Constructor potentially for use cases focused on writing (provides data segment address).
     */
    public HdfGlobalHeap(GLobalHeapDataSegmentAddress dataSegmentAddress) {
        this.initialize = null; // No lazy loading callback
        this.dataSegmentAddress = dataSegmentAddress;
        this.heapCollections = new HashMap<>();
        this.collectionSizes = new HashMap<>();
        this.nextObjectIds = new HashMap<>();
    }

    /**
     * Retrieves the data bytes for a specific object within a specific global heap collection.
     *
     * @param heapOffset The starting file offset of the global heap collection.
     * @param objectId   The ID of the object within the specified heap.
     * @return The byte array data of the requested object.
     * @throws IllegalStateException If the specified heap is not loaded and cannot be initialized.
     * @throws RuntimeException      If the objectId is not found within the loaded heap, or if ID 0 is requested.
     */
    public byte[] getDataBytes(long heapOffset, int objectId) {
        if (objectId == 0) {
            throw new IllegalArgumentException("Cannot request data bytes for Global Heap Object ID 0 (null terminator)");
        }

        TreeMap<Integer, GlobalHeapObject> specificHeapObjects = heapCollections.get(heapOffset);

        // If heap not loaded, try initializing it via callback
        if (specificHeapObjects == null) {
            if (initialize != null) {
                // Ask the caller (via callback) to load the specific heap at heapOffset
                initialize.initializeCallback(heapOffset);

                // Check again if the callback loaded the heap
                specificHeapObjects = heapCollections.get(heapOffset);
                if (specificHeapObjects == null) {
                    throw new IllegalStateException("Heap not found or loaded for offset: " + heapOffset + " even after initialization callback.");
                }
            } else {
                // Heap not loaded and no way to initialize it
                throw new IllegalStateException("Heap not loaded for offset: " + heapOffset + " and no initializer provided.");
            }
        }

        // Heap is loaded, now find the object
        GlobalHeapObject obj = specificHeapObjects.get(objectId);
        if (obj == null) {
            throw new RuntimeException("No object found for objectId: " + objectId + " in heap at offset: " + heapOffset);
        }
        // Object ID 0 should not be in the map if read correctly, but double-check
        if (obj.getObjectId() == 0) {
            throw new RuntimeException("Internal error: Object ID 0 found unexpectedly during data retrieval for offset: " + heapOffset);
        }
        return obj.getData();
    }

    /**
     * Reads a single global heap collection starting from the fileChannel's current position.
     * Stores the read objects mapped to their starting file offset.
     *
     * @param fileChannel     The FileChannel positioned at the start of the global heap.
     * @param ignoredOffsetSize Ignored parameter (kept for signature compatibility if needed elsewhere).
     * @throws IOException              If an I/O error occurs.
     * @throws IllegalArgumentException If the heap signature or version is invalid.
     * @throws RuntimeException         If invalid data structures (e.g., sizes, duplicates) are found within the heap.
     */
    public void readFromFileChannel(FileChannel fileChannel, short ignoredOffsetSize) throws IOException {
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

        if (declaredSize < 16 || declaredSize > fileChannel.size() - startOffset) {
            throw new IllegalArgumentException("Invalid declared collection size: " + declaredSize + " at offset: " + startOffset);
        }
        // Size of object data buffer = declared total size - header size
        // This buffer includes space for all object headers, data, padding, AND the final null terminator header,
        // plus any remaining unused space within the declaredSize.
        int objectDataBufferSize = (int) (declaredSize - 16);
        if (objectDataBufferSize < 0) {
            // Need at least space for the null terminator header (16 bytes) if no objects
            // Allow 0 if declaredSize is exactly 16 (empty heap, only header). This seems unlikely.
            // A heap must contain at least the null terminator object entry (16 bytes).
            // So minimum valid objectDataBufferSize is 16? Let's assume >= 0 is okay for now.
            // throw new IllegalArgumentException("Calculated object data size is negative: " + objectDataBufferSize + " at offset: " + startOffset);
            if (objectDataBufferSize < 0) { // Stricter check: needs space for null term
                throw new IllegalArgumentException("Declared size " + declaredSize + " is too small for heap header at offset " + startOffset);
            }
        }

        ByteBuffer objectBuffer = ByteBuffer.allocate(objectDataBufferSize);
        objectBuffer.order(ByteOrder.LITTLE_ENDIAN);
        bytesRead = fileChannel.read(objectBuffer);
        if (bytesRead < objectDataBufferSize) {
            throw new IOException("Failed to read complete global heap object data buffer ("+bytesRead+"/"+objectDataBufferSize+") at offset: " + startOffset);
        }
        objectBuffer.flip();

        // --- Read objects into a local map for this specific heap ---
        TreeMap<Integer, GlobalHeapObject> localObjects = new TreeMap<>();
        int localNextObjectId = 1;

        try {
            while (objectBuffer.hasRemaining()) {
                // Check if there's enough space for at least an object header before reading
                if (objectBuffer.remaining() < 16) {
                    throw new RuntimeException("Insufficient remaining buffer space ("+objectBuffer.remaining()+") for object header in heap at offset " + startOffset);
                }

                GlobalHeapObject obj = GlobalHeapObject.readFromByteBuffer(objectBuffer);

                // Check for the null terminator object
                if (obj.getObjectId() == 0) {
                    // Found terminator. We don't store it in the map.
                    // The loop terminates. Any remaining bytes in objectBuffer are
                    // unused space within the declared collection size. This is NOT an error.
                    break; // Stop reading objects for this heap
                }

                // Check for duplicate object IDs within the same heap
                if (localObjects.containsKey(obj.getObjectId())) {
                    throw new RuntimeException("Duplicate object ID " + obj.getObjectId() + " found in heap at offset: " + startOffset);
                }

                localObjects.put(obj.getObjectId(), obj);
                localNextObjectId = Math.max(localNextObjectId, obj.getObjectId() + 1);
            }
        } catch (RuntimeException e) { // Catch runtime exceptions from readFromByteBuffer or duplicate check
            throw new RuntimeException("Error processing global heap object data at offset: " + startOffset + ": " + e.getMessage(), e);
        } catch (Exception e) { // Catch other potential exceptions during buffer processing
            throw new IOException("Unexpected error processing global heap object data buffer at offset: " + startOffset, e);
        }


        // --- Store the successfully read heap data mapped by its start offset ---
        this.heapCollections.put(startOffset, localObjects);
        this.collectionSizes.put(startOffset, localCollectionSize); // Store size if needed later
        this.nextObjectIds.put(startOffset, localNextObjectId); // Store next ID if needed later
    }

    // ========================================================================
    // Methods below are NOT functionally updated for multiple heaps yet.
    // Commented code retained for reference.
    // ========================================================================

    /**
     * WARNING: This method is NOT updated for multiple heaps and will throw an exception.
     * Adds data to a *new* heap object. The logic for managing multiple heap sizes,
     * offsets, and object IDs needs implementation.
     *
     * @param bytes Data to add.
     * @return Placeholder or throws exception.
     */
    public byte[] addToHeap(byte[] bytes) {
        throw new UnsupportedOperationException("addToHeap is not implemented for multiple heap collections yet.");

        /* --- Old Logic (for reference, needs adaptation) ---
        // This assumes 'objects', 'nextObjectId', 'collectionSize', 'dataSegmentOffset' are single members
        // They need to become lookups/updates based on a heapOffset parameter.

        // Example adaptations needed:
        // long heapOffset = ...; // Determine target heap
        // TreeMap<Integer, GlobalHeapObject> targetObjects = heapCollections.computeIfAbsent(heapOffset, k -> new TreeMap<>());
        // int currentNextId = nextObjectIds.getOrDefault(heapOffset, 1);
        // HdfFixedPoint currentCollectionSize = collectionSizes.get(heapOffset); // Needs initial value handling
        // long currentDataSegmentOffset = dataSegmentOffset.getInstance(Long.class); // This member itself needs rethinking for multi-heap

        if (objects == null) { // This check is invalid, use map lookup
            objects = new TreeMap<>(); // Invalid initialization
        }
        int objectSize = bytes.length;
        int alignedSize = alignToEightBytes(objectSize); // Padding for data *within* the object struct
        int headerSize = 16; // Size of GlobalHeapObject header

        if (nextObjectId > 0xFFFF) { // Check needs to use currentNextId
            throw new IllegalStateException("Maximum number of global heap objects exceeded.");
        }

        // Size calculation needs adaptation based on currentCollectionSize
        long newSize = collectionSize.getInstance(Long.class) + headerSize + alignedSize; // Logic likely needs adjustment
        this.collectionSize = HdfFixedPoint.of(newSize); // Needs map update: collectionSizes.put(heapOffset, HdfFixedPoint.of(newSize));

        // Use currentNextId, and update it in the map
        GlobalHeapObject obj = new GlobalHeapObject(currentNextId, 0, objectSize, bytes); // Assumes the correct constructor exists
        targetObjects.put(currentNextId, obj);
        int objectId = currentNextId; // Local copy for return buffer
        nextObjectIds.put(heapOffset, currentNextId + 1); // Update next ID for this heap

        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(objectSize); // This seems unrelated to the heap object, maybe object header? Check HDF5 format.
                                   // This part seems to be creating the *reference* to the heap object, not the object itself.
        if (dataSegmentAddress == null) throw new IllegalStateException("Data Segment Address provider missing for addToHeap");
        buffer.putLong(dataSegmentAddress.getDataSegmentAddress()); // Should this be heapOffset? Check format.
        buffer.putInt(objectId);
        return buffer.array();
         */
    }

    /**
     * WARNING: This method is NOT updated for multiple heaps and will throw an exception.
     * Writes a *specific* heap collection to the buffer. Needs offset parameter to identify which heap.
     *
     * @param buffer The ByteBuffer to write to.
     */
    public void writeToByteBuffer(ByteBuffer buffer) {
        throw new UnsupportedOperationException("writeToByteBuffer is not implemented for multiple heap collections yet.");

        /* --- Old Logic (for reference, needs adaptation) ---
        // Need heapOffset parameter
        long heapOffset = 0; // Example: must be provided
        TreeMap<Integer, GlobalHeapObject> objectsToWrite = this.heapCollections.get(heapOffset);
        HdfFixedPoint sizeToWrite = this.collectionSizes.get(heapOffset); // Get size associated with this heap offset

        if (objectsToWrite == null) { // Check if heap for offset exists
            // Should probably not throw, maybe write nothing or handle based on context?
            // For now, matches original behavior conceptually.
            throw new IllegalStateException("Heap not initialized for offset " + heapOffset + "; cannot write.");
        }

        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(SIGNATURE.getBytes());
        buffer.put((byte) VERSION);
        buffer.put(new byte[3]); // Reserved

        // Calculate the *actual* used size for the header field
        long actualUsedSize = 16; // Header size
        for (GlobalHeapObject obj : objectsToWrite.values()) {
            if (obj.getObjectId() == 0) continue; // Should not be in map anyway
            actualUsedSize += 16; // Object header
            actualUsedSize += obj.getObjectSize(); // Object data
            actualUsedSize += getPadding((int)obj.getObjectSize()); // Object padding // Cast ok because objectSize for ID!=0 fits int
        }
        actualUsedSize += 16; // Size of the null terminator object header (data size is 0)

        // Write the calculated actual size using HdfFixedPoint logic (assuming it handles 8 bytes)
        // Replace buffer.putLong(totalSize) from very old code with this:
        HdfFixedPoint actualSizeFixedPoint = HdfFixedPoint.of(actualUsedSize);
        actualSizeFixedPoint.writeToByteBuffer(buffer, (short) 8, new BitSet(), (short) 0, (short) 64); // Assuming parameters are correct

        // Write all actual objects
        for (GlobalHeapObject obj : objectsToWrite.values()) {
            obj.writeToByteBuffer(buffer);
        }

        // Write the null terminator entry (ID 0)
        // Need to determine the free space. This requires knowing the allocated block size (e.g. 4096).
        long allocatedSize = calculateAlignedTotalSize(heapOffset); // Get the size the buffer *should* have (aligned)
        long freeSpace = allocatedSize - actualUsedSize; // Free space is alignment padding size? Maybe? Check HDF5 spec/impl.
                                                         // It's the value read from ID 0's size field if just read.
                                                         // If *creating*, needs calculation based on allocated block.

        buffer.putShort((short) 0); // Object ID 0
        buffer.putShort((short) 0); // Reference count 0
        buffer.putInt(0); // Reserved
        buffer.putLong(freeSpace); // Write calculated/known free space for ID 0 size field

        // Padding after the null terminator up to the alignment boundary is implicitly handled
        // by ensuring the buffer passed *into* this method has the correct total aligned size
        // calculated by calculateAlignedTotalSize / getWriteBufferSize.
        // This method writes 'actualUsedSize' bytes + null terminator header bytes.
        */
    }

    /**
     * WARNING: This method is NOT updated for multiple heaps and will throw an exception.
     * Calculates the required buffer size for writing a specific heap, including alignment padding.
     */
    private long calculateAlignedTotalSize(long heapOffset) {
        throw new UnsupportedOperationException("calculateAlignedTotalSize is not implemented for multiple heap collections yet.");

        /* --- Old Logic (needs adaptation) ---
        TreeMap<Integer, GlobalHeapObject> objectsToCalc = this.heapCollections.get(heapOffset);
        if ( objectsToCalc == null ) {
            return 0; // Or should return ALIGNMENT for an empty heap? Depends on HDF5 requirements.
                      // Let's assume 0 is valid if heap doesn't exist for offset.
        }

        // Calculate actual size used by header and all objects (including null terminator header)
        long actualUsedSize = 16; // Header size
        for (GlobalHeapObject obj : objectsToCalc.values()) {
            // if (obj.getObjectId() == 0) continue; // Objects map doesn't contain ID 0
            actualUsedSize += 16; // Object header
            actualUsedSize += obj.getObjectSize(); // Object data
            actualUsedSize += getPadding((int)obj.getObjectSize()); // Object padding // Cast OK
        }
        actualUsedSize += 16; // Add size of the null terminator object header (data size 0)

        // Return the size aligned to the required boundary (e.g., 4096)
        // This is the total buffer size needed.
        return alignTo(actualUsedSize, ALIGNMENT);
         */
    }

    /**
     * WARNING: This method is NOT updated for multiple heaps and will throw an exception.
     * Gets the required buffer size for writing a specific heap.
     */
    public long getWriteBufferSize(long heapOffset) {
        throw new UnsupportedOperationException("getWriteBufferSize is not implemented for multiple heap collections yet.");
        // return calculateAlignedTotalSize(heapOffset); // Needs implementation that calls adapted version
    }


    // ========================================================================
    // Helper Methods & Interfaces
    // ========================================================================

    private static long alignTo(long size, int alignment) {
        if (alignment <= 0 || (alignment & (alignment - 1)) != 0) {
            throw new IllegalArgumentException("Alignment must be a positive power of 2. Got: " + alignment);
        }
        return (size + alignment - 1) & ~((long)alignment - 1);
    }

    private static int getPadding(int size) {
        // Padding to make object data + padding a multiple of 8
        return (8 - (size % 8)) % 8;
    }

    private static int alignToEightBytes(int size) {
        // Used only in unimplemented addToHeap logic for now
        // Calculates size aligned up to the nearest multiple of 8
        return (size + 7) & ~7;
    }

    @Override
    public String toString() {
        return "HdfGlobalHeap{" +
                "loadedHeapCount=" + heapCollections.size() +
                ", knownOffsets=" + heapCollections.keySet() +
                '}';
    }

    public interface GlobalHeapInitialize {
        /**
         * Called when data is requested from a heap that hasn't been loaded yet.
         * The implementation should typically position a FileChannel to heapOffset
         * and call HdfGlobalHeap.readFromFileChannel().
         *
         * @param heapOffset The starting file offset of the global heap to load.
         */
        void initializeCallback(long heapOffset);
    }

    public interface GLobalHeapDataSegmentAddress {
        long getDataSegmentAddress();
    }

    /**
     * Inner class representing a single object within a global heap.
     * Reads the object structure according to HDF5 spec.
     */
    @Getter
    private static class GlobalHeapObject {
        private final int objectId;
        private final int referenceCount;
        // For non-zero IDs: Size of object data.
        // For ID 0: Represents the free space remaining in the heap segment (read from file).
        private final long objectSize; // Stores the value read from the 8-byte size/free-space field
        private final byte[] data; // Actual data for non-zero IDs, empty for ID 0.

        /**
         * Constructor used by readFromByteBuffer.
         */
        private GlobalHeapObject(int objectId, int referenceCount, long sizeOrFreeSpace, byte[] data) {
            this.objectId = objectId;
            this.referenceCount = referenceCount;
            this.objectSize = sizeOrFreeSpace;
            this.data = (data != null) ? data : new byte[0];
        }

        /**
         * Reads one Global Heap Object entry from the buffer.
         * Advances the buffer position past the object entry (including padding for non-zero IDs).
         * Handles the special meaning of the size field for Object ID 0.
         *
         * @param buffer ByteBuffer containing object data, ordered LITTLE_ENDIAN.
         * @return The parsed GlobalHeapObject.
         * @throws RuntimeException If buffer is too small or invalid data is encountered.
         */
        public static GlobalHeapObject readFromByteBuffer(ByteBuffer buffer) {
            if (buffer.remaining() < 16) {
                throw new RuntimeException("Buffer underflow: insufficient data for Global Heap Object header (needs 16 bytes, found " + buffer.remaining() + ")");
            }
            int objectId = Short.toUnsignedInt(buffer.getShort());
            int referenceCount = Short.toUnsignedInt(buffer.getShort());
            buffer.getInt(); // Skip 4 reserved bytes
            long sizeOrFreeSpace = buffer.getLong(); // Read the 8-byte field.

            if (objectId == 0) {
                // Null terminator object (Object ID 0)
                // The 'sizeOrFreeSpace' field indicates the free space remaining in the heap.
                if (sizeOrFreeSpace < 0) {
                    throw new RuntimeException("Invalid negative free space (" + sizeOrFreeSpace + ") indicated by null terminator object (ID 0).");
                }
                // No data or padding follows the header for object ID 0.
                return new GlobalHeapObject(objectId, referenceCount, sizeOrFreeSpace, null); // data is null

            } else {
                // Regular object (Object ID != 0)
                // The field read is the actual object data size.
                if (sizeOrFreeSpace <= 0) {
                    throw new RuntimeException("Invalid non-positive object size (" + sizeOrFreeSpace + ") read for non-null object ID: " + objectId);
                }
                if (sizeOrFreeSpace > Integer.MAX_VALUE) { // Check if size fits in int for array allocation
                    throw new RuntimeException("Object size (" + sizeOrFreeSpace + ") exceeds maximum Java array size (Integer.MAX_VALUE) for object ID: " + objectId);
                }
                int actualObjectSize = (int) sizeOrFreeSpace; // Safe to cast now

                // Check buffer has enough space for data AND padding
                int padding = getPadding(actualObjectSize);
                int requiredBytes = actualObjectSize + padding;
                if (buffer.remaining() < requiredBytes) {
                    throw new RuntimeException("Buffer underflow: insufficient data for object content and padding (needs "
                            + requiredBytes + " bytes [data:" + actualObjectSize + ", pad:" + padding + "], found "
                            + buffer.remaining() + ") for object ID: " + objectId);
                }

                // Read the actual data
                byte[] objectData = new byte[actualObjectSize];
                buffer.get(objectData);

                // Consume (skip) padding bytes following the data
                if (padding > 0) {
                    buffer.position(buffer.position() + padding);
                }
                // For non-zero ID objects, the stored 'objectSize' field correctly represents data size
                return new GlobalHeapObject(objectId, referenceCount, sizeOrFreeSpace, objectData);
            }
        }

        /**
         * Writes this object's entry to the buffer.
         * NOTE: This logic might need refinement for writing ID 0 correctly
         * based on how free space is calculated/tracked during write operations.
         * It currently writes the stored objectSize value.
         *
         * @param buffer ByteBuffer to write to, ordered LITTLE_ENDIAN.
         * @throws IllegalStateException if object state is invalid for writing.
         */
        public void writeToByteBuffer(ByteBuffer buffer) {
            buffer.putShort((short) objectId);
            buffer.putShort((short) referenceCount);
            buffer.putInt(0); // Reserved
            buffer.putLong(objectSize); // Writes size for non-zero ID, free space for ID 0

            if (objectId != 0) {
                // Only write data and padding for non-zero IDs
                // Perform check before writing data
                int expectedDataSize = (int)objectSize; // Cast OK since read checked against MAX_INT
                if (data == null || data.length != expectedDataSize) {
                    throw new IllegalStateException("Object data is inconsistent or null for writing object ID: " + objectId + ". Expected size " + expectedDataSize + ", data length " + (data != null ? data.length : "null"));
                }
                buffer.put(data);
                int padding = getPadding(expectedDataSize);
                if (padding > 0) {
                    buffer.put(new byte[padding]);
                }
            }
            // No data or padding written for object ID 0 itself
        }
    } // End GlobalHeapObject inner class

} // End HdfGlobalHeap class