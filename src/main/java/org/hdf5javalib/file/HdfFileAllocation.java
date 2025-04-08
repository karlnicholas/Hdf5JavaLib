package org.hdf5javalib.file; // Adjust package if needed

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedHashMap; // Use LinkedHashMap to preserve order
import java.util.Map;
import java.util.Objects;

/**
 * Singleton class managing the allocation layout (offsets and sizes) via recalculation.
 * Responsible for all allocation actions and updating DatasetAllocationInfo state.
 * Supports increasing dataset header size *before* any data block is allocated,
 * triggering a layout recalculation.
 *
 * *** Attempt to restore functionality including getLocalHeapOffset ***
 */
@Getter
@Slf4j
public final class HdfFileAllocation {

    @Getter
    private static final HdfFileAllocation instance = new HdfFileAllocation();

    // --- Constants ---
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
    public static final long GLOBAL_HEAP_BLOCK_SIZE = 4096L;
    private static final long DEFAULT_DATASET_HEADER_ALLOCATION_SIZE = DATA_OBJECT_HEADER_MESSAGE_SIZE + 256L; // 272L Example
    private static final long MIN_DATA_OFFSET_THRESHOLD = 2048L;
    private final long snodStorageSize; // Calculated in constructor

    // --- Storage for Multiple Dataset Allocations ---
    private final Map<String, DatasetAllocationInfo> datasetAllocations = new LinkedHashMap<>();

    // --- Fixed Setup Block Offsets (Calculated once) ---
    private final long superblockOffset = SUPERBLOCK_OFFSET;
    private long objectHeaderPrefixOffset;
    private long btreeOffset;
    private long localHeapHeaderOffset;          // Offset of the LH *Header*
    private long initialLocalHeapContentsOffset; // Offset of the *first* LH *Contents* block
    private long snodOffset;

    // --- Fixed Setup Block Sizes ---
    private final long btreeTotalSize;
    private final long initialLocalHeapContentsSize = SETUP_LOCAL_HEAP_CONTENTS_SIZE;

    // --- Tracking for Dynamically Allocated/Sized Components ---
    private long currentLocalHeapContentsOffset; // Tracks offset of current/last LH *Contents* block
    private long currentLocalHeapContentsSize;   // Tracks size of current/last LH *Contents* block
    private long globalHeapOffset = -1L;         // Tracks offset of *first* allocated GH block

    // --- Allocation Tracking ---
    private long nextAvailableOffset;
    private boolean dataBlocksAllocated = false; // Locks header resizing

    /** Private Constructor */
    private HdfFileAllocation() {
        this.snodStorageSize = SETUP_SNOD_V1_HEADER_SIZE + (DEFAULT_SETUP_SNOD_ENTRY_COUNT * SETUP_SNOD_V1_ENTRY_SIZE);
        this.btreeTotalSize = SETUP_BTREE_NODE_SIZE + SETUP_BTREE_STORAGE_SIZE;
        calculateInitialLayout();
    }

    /** Calculates the offsets of the initial setup structures ONLY and resets state. */
    private void calculateInitialLayout() {
        log.debug("Calculating initial file layout...");
        long currentOffset = SUPERBLOCK_OFFSET + SUPERBLOCK_SIZE;
        objectHeaderPrefixOffset = currentOffset; currentOffset += SETUP_OBJECT_HEADER_PREFIX_SIZE;
        btreeOffset = currentOffset; currentOffset += btreeTotalSize;
        localHeapHeaderOffset = currentOffset; currentOffset += SETUP_LOCAL_HEAP_HEADER_SIZE; // Assign LH Header offset
        initialLocalHeapContentsOffset = currentOffset; currentOffset += initialLocalHeapContentsSize; // Assign *initial* LH Contents offset
        snodOffset = currentOffset; currentOffset += snodStorageSize;
        long endOfSetupBlock = currentOffset;
        log.debug("End of fixed setup block calculated at: {}", endOfSetupBlock);

        currentLocalHeapContentsOffset = initialLocalHeapContentsOffset; // Track initial LH contents offset
        currentLocalHeapContentsSize = initialLocalHeapContentsSize;     // Track initial LH contents size
        globalHeapOffset = -1L;
        this.datasetAllocations.clear();
        this.dataBlocksAllocated = false;
        this.nextAvailableOffset = Math.max(MIN_DATA_OFFSET_THRESHOLD, endOfSetupBlock);
        log.debug("Initial layout complete. Next available offset set to: {}", this.nextAvailableOffset);
    }

    // --- Recalculation Core ---
    /** Recalculates layout based on current sizes. Called when headers resize before data lock. */
    private synchronized void recalculateLayout() {
        if (dataBlocksAllocated) { log.error("Internal error: recalculateLayout called after dataBlocksAllocated is true."); return; }
        log.debug("Recalculating dynamic layout...");
        long currentOffset = snodOffset + snodStorageSize; // Start after last fixed setup block
        currentOffset = Math.max(MIN_DATA_OFFSET_THRESHOLD, currentOffset);
        log.trace("Recalc starting offset: {}", currentOffset);

        // Order: Dataset Headers -> Global Heap -> Expanded Local Heap -> Continuations
        for (DatasetAllocationInfo info : datasetAllocations.values()) {
            info.setHeaderOffset(currentOffset);
            log.trace(" Recalc Dataset Header '{}': Offset={}, Size={}", getDatasetName(info), currentOffset, info.getHeaderSize());
            currentOffset += info.getHeaderSize();
        }
        if (globalHeapOffset != -1L) { // Place first allocated GH block
            globalHeapOffset = currentOffset;
            log.trace(" Recalc Global Heap: Offset={}, Size={}", currentOffset, GLOBAL_HEAP_BLOCK_SIZE);
            currentOffset += GLOBAL_HEAP_BLOCK_SIZE;
        }
        // Place *expanded* LH contents block (if it's not the initial one)
        if (currentLocalHeapContentsOffset != initialLocalHeapContentsOffset) {
            currentLocalHeapContentsOffset = currentOffset;
            log.trace(" Recalc Expanded Local Heap: Offset={}, Size={}", currentOffset, currentLocalHeapContentsSize);
            currentOffset += currentLocalHeapContentsSize;
        } else if (currentLocalHeapContentsSize != initialLocalHeapContentsSize) {
            log.warn("Recalc found local heap size mismatch but offset matches initial. Size={}, Offset={}", currentLocalHeapContentsSize, currentLocalHeapContentsOffset);
        }
        // Add other dynamic block recalc logic here...
        for (DatasetAllocationInfo info : datasetAllocations.values()) { // Place allocated continuations last
            if (info.getContinuationSize() != -1L) {
                info.setContinuationOffset(currentOffset);
                log.trace(" Recalc Dataset Continuation '{}': Offset={}, Size={}", getDatasetName(info), currentOffset, info.getContinuationSize());
                currentOffset += info.getContinuationSize();
            }
        }
        nextAvailableOffset = currentOffset;
        log.debug("Recalculation complete. Next available offset: {}", nextAvailableOffset);
    }

    // Helper to find dataset name (for logging)
    private String getDatasetName(DatasetAllocationInfo targetInfo) {
        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            if (entry.getValue() == targetInfo) return entry.getKey();
        }
        return "[Unknown Dataset]";
    }

    // --- Allocation Primitives ---
    /** Internal helper to allocate a block and advance the EOA marker. */
    private synchronized long allocateBlock(long size) {
        if (size < 0L) throw new IllegalArgumentException("Allocation size cannot be negative.");
        long offset = nextAvailableOffset;
        nextAvailableOffset += size;
        return offset;
    }

    // --- PUBLIC ALLOCATION METHODS ---

    /** Allocates a generic block of space. */
    public synchronized long allocateGenericBlock(long size) {
        log.debug("Allocating generic block (size {})", size);
        return allocateBlock(size);
    }

    /** Allocates space for a Message Continuation block. */
    public synchronized long allocateNextMessageContinuation(long continuationSize) {
        if (continuationSize <= 0L) throw new IllegalArgumentException("Continuation size must be positive.");
        log.debug("Allocating next Message Continuation block (size {})", continuationSize);
        return allocateBlock(continuationSize);
    }

    /** Allocates space for Dataset raw data. */
    public synchronized long allocateDataObjectData(long dataSize) {
        if (dataSize < 0L) throw new IllegalArgumentException("Data size cannot be negative.");
        return allocateBlock(dataSize);
    }

    /** Allocates space for a new SNOD block. */
    public synchronized long allocateNextSnodStorage() {
        long allocationSize = this.snodStorageSize;
        if (allocationSize <= 0L) throw new IllegalStateException("Setup SNOD storage size is not valid: " + allocationSize);
        log.debug("Allocating next SNOD storage block (size {})", allocationSize);
        return allocateBlock(allocationSize);
    }

    /** Expands storage for Local Heap contents, allocating a new block. */
    public synchronized long expandLocalHeapContents() {
        if (dataBlocksAllocated) log.warn("Expanding local heap after data lock.");
        long oldSize = this.currentLocalHeapContentsSize;
        if (oldSize <= 0L) throw new IllegalStateException("Cannot expand heap with non-positive current tracked size: " + oldSize);
        long newSize = oldSize * 2L;
        log.debug("Expanding Local Heap Contents: oldTrackedSize={}, newSize={}", oldSize, newSize);
        long newOffset = allocateBlock(newSize);
        this.currentLocalHeapContentsOffset = newOffset;
        this.currentLocalHeapContentsSize = newSize;
        log.info("Local Heap contents expanded. New active block: offset={}, size={}", newOffset, newSize);
        return newOffset;
    }

    /** Allocates space for a generic Object Header block. */
    public synchronized long allocateNextObjectHeader(long totalHeaderSize) {
        if (totalHeaderSize <= 0L) throw new IllegalArgumentException("Object Header total size must be positive.");
        log.debug("Allocating next generic Object Header block (size {})", totalHeaderSize);
        return allocateBlock(totalHeaderSize);
    }

    /** Allocates the first Global Heap block. */
    public synchronized long allocateFirstGlobalHeapBlock() {
        if (globalHeapOffset != -1L) throw new IllegalStateException("Global heap already allocated at " + globalHeapOffset);
        if (dataBlocksAllocated) log.warn("Allocating global heap after data lock.");
        long offset = allocateBlock(GLOBAL_HEAP_BLOCK_SIZE);
        this.globalHeapOffset = offset;
        log.debug("Allocated first Global Heap block @{} (size {})", offset, GLOBAL_HEAP_BLOCK_SIZE);
        return offset;
    }

    /** Allocates a subsequent Global Heap block. */
    public synchronized long allocateNextGlobalHeapBlock() {
        if (dataBlocksAllocated) log.warn("Allocating next global heap block after data lock.");
        long newHeapOffset = allocateBlock(GLOBAL_HEAP_BLOCK_SIZE);
        log.debug("Allocated next Global Heap block @{} (size {})", newHeapOffset, GLOBAL_HEAP_BLOCK_SIZE);
        return newHeapOffset;
    }

    // --- Dataset Specific Allocation Methods ---

    /** Allocates initial space for a dataset's object header. */
    public synchronized DatasetAllocationInfo allocateDatasetStorage(String datasetName) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (datasetName.isEmpty()) throw new IllegalArgumentException("Dataset name cannot be empty");
        if (datasetAllocations.containsKey(datasetName)) throw new IllegalStateException("Dataset '" + datasetName + "' already allocated.");
        if (dataBlocksAllocated) log.warn("Allocating dataset storage after data lock; header resizing disabled.");
        long headerAllocSize = DEFAULT_DATASET_HEADER_ALLOCATION_SIZE;
        long headerOffset = allocateBlock(headerAllocSize);
        DatasetAllocationInfo info = new DatasetAllocationInfo(headerOffset, headerAllocSize);
        datasetAllocations.put(datasetName, info);
        log.debug("Allocated initial dataset storage for '{}': {}", datasetName, info);
        return info;
    }

    /** Increases header size and triggers layout recalculation (before data lock). */
    public synchronized void increaseHeaderAllocation(String datasetName, long newTotalHeaderSize) {
        log.warn("Attempting to increase header allocation for '{}' to {} bytes.", datasetName, newTotalHeaderSize);
        if (dataBlocksAllocated) throw new IllegalStateException("Cannot increase header allocation after data blocks have been allocated.");
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        DatasetAllocationInfo targetInfo = datasetAllocations.get(datasetName);
        if (targetInfo == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (newTotalHeaderSize <= targetInfo.getHeaderSize()) throw new IllegalArgumentException("New size must be greater than current size.");
        targetInfo.setHeaderSize(newTotalHeaderSize);
        recalculateLayout();
        log.info("Successfully increased header size for '{}' and recalculated layout.", datasetName);
    }

    /** Allocates the data block, updates info, and sets the global header resize lock. */
    public synchronized long allocateAndSetDataBlock(String datasetName, long dataSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (dataSize < 0L) throw new IllegalArgumentException("Data size cannot be negative.");
        DatasetAllocationInfo info = datasetAllocations.get(datasetName);
        if (info == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (info.getDataOffset() != -1L || info.getDataSize() != -1L) throw new IllegalStateException("Data block for '" + datasetName + "' already allocated.");
        long dataOffset = allocateBlock(dataSize);
        info.setDataOffset(dataOffset);
        info.setDataSize(dataSize);
        if (!this.dataBlocksAllocated) {
            log.warn("First data block allocated (dataset '{}'). Header resizing is now disabled globally.", datasetName);
            this.dataBlocksAllocated = true;
        }
        log.debug("Allocated and set data block for dataset '{}': Offset={}, Size={}", datasetName, dataOffset, dataSize);
        return dataOffset;
    }

    /** Allocates the continuation block and updates info. */
    public synchronized long allocateAndSetContinuationBlock(String datasetName, long continuationSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (continuationSize <= 0L) throw new IllegalArgumentException("Continuation size must be positive.");
        DatasetAllocationInfo info = datasetAllocations.get(datasetName);
        if (info == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (info.getContinuationSize() != -1L) throw new IllegalStateException("Continuation block for '" + datasetName + "' already allocated/set.");
        if (dataBlocksAllocated) log.warn("Allocating continuation block after data lock.");
        long continuationOffset = allocateBlock(continuationSize);
        info.setContinuationOffset(continuationOffset);
        info.setContinuationSize(continuationSize);
        log.debug("Allocated and set continuation block for dataset '{}': Offset={}, Size={}", datasetName, info.getContinuationOffset(), info.getContinuationSize());
        return info.getContinuationOffset();
    }

    // --- Getters ---
    public synchronized DatasetAllocationInfo getDatasetAllocationInfo(String datasetName) { return datasetAllocations.get(datasetName); }
    public synchronized Map<String, DatasetAllocationInfo> getAllDatasetAllocations() { return Collections.unmodifiableMap(new LinkedHashMap<>(datasetAllocations)); }
    public synchronized long getEndOfFileOffset() { return nextAvailableOffset; }
    public long getSuperblockSize() { return SUPERBLOCK_SIZE; }
    public synchronized long getGlobalHeapOffset() { return globalHeapOffset; } // Returns offset of *first* allocated GH
    public synchronized long getCurrentLocalHeapContentsOffset() { return currentLocalHeapContentsOffset; } // Offset of *current* LH Contents
    public synchronized long getCurrentLocalHeapContentsSize() { return currentLocalHeapContentsSize; }     // Size of *current* LH Contents
    public synchronized boolean isDataBlocksAllocated() { return dataBlocksAllocated; }
    public long getObjectHeaderPrefixOffset() { return objectHeaderPrefixOffset; } // Root group OH prefix offset
    public long getBtreeOffset() { return btreeOffset; }                         // Root group BTree node offset

    // ============================================
    // === RESTORED GETTER ===
    // ============================================
    /**
     * Gets the offset of the Local Heap *Header* block allocated during initial setup.
     * @return The offset of the setup Local Heap Header.
     */
    public long getLocalHeapOffset() {
        return localHeapHeaderOffset; // Returns the calculated offset of the LH Header
    }
    // ============================================

    public long getSnodOffset() { return snodOffset; }                            // Root group SNOD offset

    // --- Utility / Reset ---
    public synchronized void reset() {
        log.warn("Resetting HdfFileAllocation state.");
        this.nextAvailableOffset = 0L;
        calculateInitialLayout();
    }
}