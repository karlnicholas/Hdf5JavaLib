package org.hdf5javalib.file; // Adjust package if needed

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Singleton class managing the allocation layout (offsets and sizes) of structures
 * within an HDF5 file during the writing process. All sizes are represented as long.
 * Manages allocations for setup blocks and dynamically added datasets/structures.
 */
@Getter
@Slf4j
public final class HdfFileAllocation {

    // --- Singleton Instance ---
    @Getter
    private static final HdfFileAllocation instance = new HdfFileAllocation();

    // --- Constants ---
    // ... (Constants remain the same)
    private static final long SUPERBLOCK_OFFSET = 0L;
    private static final long SUPERBLOCK_SIZE = 96L;
    private static final long SETUP_OBJECT_HEADER_PREFIX_SIZE = 40L;
    private static final long SETUP_BTREE_NODE_SIZE = 32L;
    private static final long SETUP_BTREE_STORAGE_SIZE = 512L;
    private static final long SETUP_LOCAL_HEAP_HEADER_SIZE = 32L;
    private static final long SETUP_LOCAL_HEAP_CONTENTS_SIZE = 88L;
    private static final long DATA_OBJECT_HEADER_MESSAGE_SIZE = 16L;
    private static final long SETUP_SNOD_V1_HEADER_SIZE = 8L;
    private static final long SETUP_SNOD_V1_ENTRY_SIZE = 32L;
    private static final long DEFAULT_SETUP_SNOD_ENTRY_COUNT = 10L;
    private static final long MIN_DATA_OFFSET_THRESHOLD = 2048L;
    public static final long GLOBAL_HEAP_BLOCK_SIZE = 4096L;
    private static final long DEFAULT_DATASET_HEADER_ALLOCATION_SIZE = DATA_OBJECT_HEADER_MESSAGE_SIZE + 256L; // 272L


    // --- Storage for Multiple Dataset Allocations ---
    private final Map<String, DatasetAllocationInfo> datasetAllocations = new HashMap<>();


    // --- Calculated Block Offsets & Sizes (Setup Layout) ---
    // ... (Setup layout fields remain the same)
    private final long superblockOffset = SUPERBLOCK_OFFSET;
    private long objectHeaderPrefixOffset;
    private long btreeOffset;
    private long localHeapOffset;
    private long localHeapContentsOffset;
    private long snodOffset;

    // --- Sizes (long) ---
    private final long btreeStorageSize = SETUP_BTREE_STORAGE_SIZE;
    private final long localHeapContentsSize = SETUP_LOCAL_HEAP_CONTENTS_SIZE;
    private final long snodStorageSize = SETUP_SNOD_V1_HEADER_SIZE + (DEFAULT_SETUP_SNOD_ENTRY_COUNT * SETUP_SNOD_V1_ENTRY_SIZE);

    // --- Tracking for Active/Expandable Structures ---
    private long currentLocalHeapContentsOffset;
    private long currentLocalHeapContentsSize;

    // --- Allocation Tracking ---
    private long nextAvailableOffset;

    // --- Global Heap Tracking ---
    private long globalHeapOffset = -1L;


    /**
     * Private Constructor
     */
    private HdfFileAllocation() {
        calculateSetupLayout();
    }

    /**
     * Calculates the offsets of the initial setup structures ONLY.
     */
    private void calculateSetupLayout() {
        // ... (Implementation remains the same)
        long currentOffset = SUPERBLOCK_OFFSET;
        currentOffset += SUPERBLOCK_SIZE;
        objectHeaderPrefixOffset = currentOffset;
        currentOffset += SETUP_OBJECT_HEADER_PREFIX_SIZE;
        btreeOffset = currentOffset;
        currentOffset += SETUP_BTREE_NODE_SIZE + btreeStorageSize; // btree node + leaf storage
        localHeapOffset = currentOffset;
        currentOffset += SETUP_LOCAL_HEAP_HEADER_SIZE;
        localHeapContentsOffset = currentOffset;
        currentOffset += localHeapContentsSize;
        snodOffset = currentOffset;
        currentOffset += snodStorageSize;
        long endOfSetupBlock = currentOffset;

        currentLocalHeapContentsOffset = localHeapContentsOffset;
        currentLocalHeapContentsSize = localHeapContentsSize;

        if (this.nextAvailableOffset > 0L) {
            this.nextAvailableOffset = Math.max(this.nextAvailableOffset, endOfSetupBlock);
        } else {
            this.nextAvailableOffset = endOfSetupBlock;
        }
        this.nextAvailableOffset = Math.max(MIN_DATA_OFFSET_THRESHOLD, this.nextAvailableOffset);
        globalHeapOffset = -1L;
        this.datasetAllocations.clear();
        log.debug("Initial setup layout calculated. End of setup block: {}, Next available offset set to: {}", endOfSetupBlock, this.nextAvailableOffset);
    }


    // --- Methods for Dynamic Allocation (Append Strategy) ---

    /**
     * Allocates a generic block of space at the next available offset. Thread-safe.
     * @param size The size of the block to allocate (can be 0).
     * @return The starting offset of the allocated block.
     * @throws IllegalArgumentException if size is negative.
     */
    public synchronized long allocateGenericBlock(long size) {
        if (size < 0L) {
            throw new IllegalArgumentException("Allocation size cannot be negative.");
        }
        long allocationOffset = nextAvailableOffset;
        nextAvailableOffset += size;
        // log.trace("Allocated generic block: offset={}, size={}, new nextAvailableOffset={}", allocationOffset, size, nextAvailableOffset);
        return allocationOffset;
    }

    // ... (allocateNextSnodStorage, allocateNextObjectHeader, allocateNextMessageContinuation remain the same)
    public synchronized long allocateNextSnodStorage() {
        long allocationSize = this.snodStorageSize;
        if (allocationSize <= 0L) {
            throw new IllegalStateException("Setup SNOD storage size is not valid (<= 0): " + allocationSize);
        }
        log.debug("Allocating next SNOD storage block (size {})", allocationSize);
        return allocateGenericBlock(allocationSize);
    }
    public synchronized long allocateNextObjectHeader(long totalHeaderSize) {
        if (totalHeaderSize <= 0L) {
            throw new IllegalArgumentException("Object Header total size must be positive.");
        }
        log.debug("Allocating next Object Header block (size {})", totalHeaderSize);
        return allocateGenericBlock(totalHeaderSize);
    }
    public synchronized long allocateNextMessageContinuation(long continuationSize) {
        if (continuationSize <= 0L) {
            throw new IllegalArgumentException("Continuation size must be positive.");
        }
        log.debug("Allocating next Message Continuation block (size {})", continuationSize);
        return allocateGenericBlock(continuationSize);
    }

    /**
     * Allocates space specifically for Dataset raw data chunk. Thread-safe.
     * NOTE: This is now primarily called by `allocateAndSetDataBlock`.
     * @param dataSize The size required for the data block (can be 0).
     * @return The starting offset of the allocated data block.
     * @throws IllegalArgumentException if dataSize is negative.
     */
    public synchronized long allocateDataObjectData(long dataSize) {
        if (dataSize < 0L) {
            throw new IllegalArgumentException("Data size cannot be negative.");
        }
        // Log moved to the calling method 'allocateAndSetDataBlock' for context
        return allocateGenericBlock(dataSize);
    }

    /**
     * Expands storage for Local Heap contents. Thread-safe.
     * @return The starting offset of the newly allocated (larger) Local Heap contents block.
     * @throws IllegalStateException if the current heap size is not positive.
     */
    public synchronized long expandLocalHeapContents() {
        // ... (Implementation remains the same)
        long oldSize = this.currentLocalHeapContentsSize;
        if (oldSize <= 0L) {
            throw new IllegalStateException("Cannot expand heap with non-positive current size: " + oldSize);
        }
        long newSize = oldSize * 2L;
        log.debug("Expanding Local Heap Contents: oldSize={}, newSize={}", oldSize, newSize);
        long newOffset = allocateGenericBlock(newSize);
        this.currentLocalHeapContentsOffset = newOffset;
        this.currentLocalHeapContentsSize = newSize;
        log.info("Local Heap contents expanded. New offset: {}, New size: {}", newOffset, newSize);
        return newOffset;
    }


    // --- Dataset Allocation Methods ---

    /**
     * Allocates space ONLY for a dataset's object header (using a default size).
     * Records the allocation info, leaving data and continuation blocks unallocated (-1).
     * Thread-safe.
     *
     * @param datasetName The unique name/path of the dataset.
     * @return DatasetAllocationInfo containing the header offset/size, with data/continuation unallocated.
     * @throws NullPointerException     if datasetName is null.
     * @throws IllegalArgumentException if datasetName is empty.
     * @throws IllegalStateException    if a dataset with the same name has already been allocated.
     */
    public synchronized DatasetAllocationInfo allocateDatasetStorage(String datasetName) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (datasetName.isEmpty()) {
            throw new IllegalArgumentException("Dataset name cannot be empty");
        }

        if (datasetAllocations.containsKey(datasetName)) {
            throw new IllegalStateException("Dataset with name '" + datasetName + "' has already been allocated.");
        }

        // Determine size for the header allocation
        long headerAllocSize = DEFAULT_DATASET_HEADER_ALLOCATION_SIZE;

        // Allocate ONLY the header block
        long headerOffset = allocateGenericBlock(headerAllocSize);

        // Create the info object with only header details initially
        DatasetAllocationInfo info = new DatasetAllocationInfo(headerOffset, headerAllocSize);
        datasetAllocations.put(datasetName, info);

        // Log the initial allocation (data/continuation will show as -1)
        log.debug("Allocated dataset storage for '{}': {}", datasetName, info);
        return info;
    }

    /**
     * Allocates the data block for a specified dataset and updates its allocation info
     * with the new offset and size. This should only be called once per dataset.
     * Thread-safe.
     *
     * @param datasetName The name/path of the dataset needing its data block allocated.
     * @param dataSize    The required size for the data block (can be 0).
     * @return The offset of the newly allocated data block.
     * @throws NullPointerException     if datasetName is null.
     * @throws IllegalArgumentException if dataSize is negative.
     * @throws IllegalStateException    if the dataset is not found or if its data block has already been allocated.
     */
    public synchronized long allocateAndSetDataBlock(String datasetName, long dataSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        // dataSize check happens in allocateDataObjectData, but good to have here too
        if (dataSize < 0L) {
            throw new IllegalArgumentException("Data size cannot be negative.");
        }

        DatasetAllocationInfo info = datasetAllocations.get(datasetName);
        if (info == null) {
            throw new IllegalStateException("Dataset '" + datasetName + "' not found for data block allocation.");
        }
        // Check if data block is already allocated (now done within info.setDataAllocation)

        // Allocate the data block
        long dataOffset = allocateDataObjectData(dataSize); // Uses the specific method

        // Update the info object with data offset and size
        try {
            info.setDataAllocation(dataOffset, dataSize);
        } catch (IllegalStateException e) {
            // This indicates a logic error - trying to allocate data twice
            log.error("Attempted to re-allocate data block for dataset '{}'. Previous: offset={}, size={}. New attempt: offset={}, size={}",
                    datasetName, info.getDataOffset(), info.getDataSize(), dataOffset, dataSize, e);
            // Re-throw the exception from setDataAllocation
            throw e;
        }

        log.debug("Allocated and set data block for dataset '{}': Offset={}, Size={}",
                datasetName, dataOffset, dataSize);
        return dataOffset;
    }


    /**
     * Allocates a continuation block for a specified dataset and updates its allocation info
     * with the new offset and size. Thread-safe.
     *
     * @param datasetName      The name/path of the dataset requiring continuation.
     * @param continuationSize The required size for the continuation block (must be positive).
     * @return The offset of the newly allocated continuation block.
     * @throws NullPointerException     if datasetName is null.
     * @throws IllegalArgumentException if continuationSize is not positive.
     * @throws IllegalStateException    if the dataset is not found or if its continuation block has already been allocated.
     */
    public synchronized long allocateAndSetContinuationBlock(String datasetName, long continuationSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (continuationSize <= 0L) {
            throw new IllegalArgumentException("Continuation size must be positive.");
        }

        DatasetAllocationInfo info = datasetAllocations.get(datasetName);
        if (info == null) {
            throw new IllegalStateException("Dataset '" + datasetName + "' not found for continuation allocation.");
        }
        // Check if already allocated is now inside info.setContinuation

        // Allocate the block
        long continuationOffset = allocateNextMessageContinuation(continuationSize);

        // Update the info object with both offset and size
        try {
            info.setContinuation(continuationOffset, continuationSize);
        } catch (IllegalStateException e) {
            log.error("Attempted to re-allocate continuation block for dataset '{}'. Previous: offset={}, size={}. New attempt: offset={}, size={}",
                    datasetName, info.getContinuationOffset(), info.getContinuationSize(), continuationOffset, continuationSize, e);
            throw e;
        }

        log.debug("Allocated and set continuation block for dataset '{}': Offset={}, Size={}",
                datasetName, continuationOffset, continuationSize);
        return continuationOffset;
    }


    /**
     * Retrieves the allocation information (offsets and sizes) for a previously allocated dataset.
     * Thread-safe for reading.
     * @param datasetName The name/path of the dataset.
     * @return The DatasetAllocationInfo, or null if not found.
     */
    public synchronized DatasetAllocationInfo getDatasetAllocationInfo(String datasetName) {
        return datasetAllocations.get(datasetName);
    }

    /**
     * Returns an unmodifiable view of the map containing all dataset allocations.
     * Thread-safe for reading the map state at the time of the call.
     * @return An unmodifiable map of dataset names to their AllocationInfo.
     */
    public synchronized Map<String, DatasetAllocationInfo> getAllDatasetAllocations() {
        return Collections.unmodifiableMap(new HashMap<>(datasetAllocations));
    }


    // --- Global Heap ---
    // ... (Global Heap methods remain the same)
    public synchronized long allocateFirstGlobalHeapBlock() {
        if (globalHeapOffset != -1L) {
            throw new IllegalStateException("First Global Heap block has already been allocated at offset " + globalHeapOffset);
        }
        long allocationOffset = allocateGenericBlock(GLOBAL_HEAP_BLOCK_SIZE);
        this.globalHeapOffset = allocationOffset;
        log.debug("Allocated first Global Heap block @{} (size {})", this.globalHeapOffset, GLOBAL_HEAP_BLOCK_SIZE);
        return this.globalHeapOffset;
    }
    public synchronized long allocateNextGlobalHeapBlock() {
        long newHeapOffset = allocateGenericBlock(GLOBAL_HEAP_BLOCK_SIZE);
        this.globalHeapOffset = newHeapOffset;
        log.debug("Allocated next Global Heap block @{} (size {})", this.globalHeapOffset, GLOBAL_HEAP_BLOCK_SIZE);
        return this.globalHeapOffset;
    }
    public synchronized long getGlobalHeapOffset() {
        return globalHeapOffset;
    }


    // --- Getters ---
    // ... (getEndOfFileOffset, getSuperblockSize remain the same)
    public synchronized long getEndOfFileOffset() {
        return nextAvailableOffset;
    }
    public long getSuperblockSize() {
        return SUPERBLOCK_SIZE;
    }


    // --- Utility / Reset ---
    /**
     * Resets the allocation state. Thread-safe.
     */
    public synchronized void reset() {
        log.warn("Resetting HdfFileAllocation state. All dynamic allocations cleared.");
        this.nextAvailableOffset = 0L; // Reset before recalculating
        calculateSetupLayout(); // This clears datasetAllocations and resets globalHeapOffset
    }

}