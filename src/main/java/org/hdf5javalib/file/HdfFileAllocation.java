package org.hdf5javalib.file;

import lombok.Getter;

import java.util.*;

/**
 * Class managing the allocation layout (offsets and sizes) via recalculation.
 * Responsible for all allocation actions and updating DatasetAllocationInfo state.
 * Supports increasing dataset header size *before* any data block is allocated,
 * triggering a layout recalculation.
 *
 * *** Attempt to restore functionality including getLocalHeapOffset ***
 */
@Getter
public final class HdfFileAllocation {

    // --- Constants ---
    private static final long SUPERBLOCK_OFFSET = 0L;
    private static final long SUPERBLOCK_SIZE = 96L;
    private static final long SETUP_OBJECT_HEADER_PREFIX_SIZE = 40L;
    private static final long SETUP_BTREE_NODE_SIZE = 32L;
    private static final long SETUP_BTREE_STORAGE_SIZE = 512L;
    private static final long SETUP_LOCAL_HEAP_HEADER_SIZE = 32L;
    private static final long SETUP_LOCAL_HEAP_CONTENTS_SIZE = 88L;
    public static final long DATA_OBJECT_HEADER_MESSAGE_SIZE = 16L;
    private static final long SETUP_SNOD_V1_HEADER_SIZE = 8L;
    private static final long SETUP_SNOD_V1_ENTRY_SIZE = 32L;
    private static final long DEFAULT_SETUP_SNOD_ENTRY_COUNT = 10L;
    public static final long GLOBAL_HEAP_BLOCK_SIZE = 4096L;
    private static final long DEFAULT_DATASET_HEADER_ALLOCATION_SIZE = DATA_OBJECT_HEADER_MESSAGE_SIZE + 256L; // 272L Example
    private static final long MIN_DATA_OFFSET_THRESHOLD = 2048L;
    @Getter
    private static final long SNOD_STORAGE_SIZE = SETUP_SNOD_V1_HEADER_SIZE + (DEFAULT_SETUP_SNOD_ENTRY_COUNT * SETUP_SNOD_V1_ENTRY_SIZE); // 328L

    // --- Storage for Multiple Dataset Allocations ---
    private final Map<String, DatasetAllocationInfo> datasetAllocations = new LinkedHashMap<>();

    // --- Storage for SNOD Allocations (Offsets Only) ---
    private final List<Long> snodAllocationOffsets = new ArrayList<>();

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

    /** Default Constructor */
    public HdfFileAllocation() {
        this.btreeTotalSize = SETUP_BTREE_NODE_SIZE + SETUP_BTREE_STORAGE_SIZE;
        calculateInitialLayout();
    }

    /** Calculates the offsets of the initial setup structures ONLY and resets state. */
    private void calculateInitialLayout() {
        long currentOffset = SUPERBLOCK_OFFSET + SUPERBLOCK_SIZE;
        objectHeaderPrefixOffset = currentOffset; currentOffset += SETUP_OBJECT_HEADER_PREFIX_SIZE;
        btreeOffset = currentOffset; currentOffset += btreeTotalSize;
        localHeapHeaderOffset = currentOffset; currentOffset += SETUP_LOCAL_HEAP_HEADER_SIZE; // Assign LH Header offset
        initialLocalHeapContentsOffset = currentOffset; currentOffset += initialLocalHeapContentsSize; // Assign *initial* LH Contents offset
        // Do not set snodOffset here; defer until after first dataset header allocation
        currentLocalHeapContentsOffset = initialLocalHeapContentsOffset; // Track initial LH contents offset
        currentLocalHeapContentsSize = initialLocalHeapContentsSize;     // Track initial LH contents size
        globalHeapOffset = -1L;
        this.datasetAllocations.clear();
        this.snodAllocationOffsets.clear(); // Reset SNOD allocation offsets
        this.dataBlocksAllocated = false;
        this.nextAvailableOffset = currentOffset; // Set to 800 initially, before SNOD or MIN_DATA_OFFSET_THRESHOLD
    }

    // --- Recalculation Core ---
    /** Recalculates layout based on current sizes. Called when headers resize before data lock. */
    private void recalculateLayout() {
        if (dataBlocksAllocated) {
            throw new IllegalStateException("Cannot recalculate layout after data blocks have been allocated.");
        }
        long currentOffset = initialLocalHeapContentsOffset + initialLocalHeapContentsSize; // Start after initial local heap contents (800)

        // Order: Dataset Headers -> SNOD -> Global Heap -> Expanded Local Heap -> Continuations
        for (DatasetAllocationInfo info : datasetAllocations.values()) {
            info.setHeaderOffset(currentOffset);
            currentOffset += info.getHeaderSize();
        }
        // Place initial SNOD after all dataset headers
        snodOffset = currentOffset;
        currentOffset += SNOD_STORAGE_SIZE;
        // Apply MIN_DATA_OFFSET_THRESHOLD only for data or later blocks
        currentOffset = Math.max(MIN_DATA_OFFSET_THRESHOLD, currentOffset);

        if (globalHeapOffset != -1L) { // Place first allocated GH block
            globalHeapOffset = currentOffset;
            currentOffset += GLOBAL_HEAP_BLOCK_SIZE;
        }
        // Place *expanded* LH contents block (if it's not the initial one)
        if (currentLocalHeapContentsOffset != initialLocalHeapContentsOffset) {
            currentLocalHeapContentsOffset = currentOffset;
            currentOffset += currentLocalHeapContentsSize;
        } else if (currentLocalHeapContentsSize != initialLocalHeapContentsSize) {
            throw new IllegalStateException("Local heap size mismatch detected with initial offset.");
        }
        // Place allocated continuations last
        for (DatasetAllocationInfo info : datasetAllocations.values()) {
            if (info.getContinuationSize() != -1L) {
                info.setContinuationOffset(currentOffset);
                currentOffset += info.getContinuationSize();
            }
        }
        nextAvailableOffset = currentOffset;
    }

    // Helper to find dataset name (no longer used for logging, retained for potential future use)
    private String getDatasetName(DatasetAllocationInfo targetInfo) {
        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            if (entry.getValue() == targetInfo) return entry.getKey();
        }
        return "[Unknown Dataset]";
    }

    // --- Allocation Primitives ---
    /** Internal helper to allocate a block and advance the EOA marker. */
    private long allocateBlock(long size) {
        if (size < 0L) throw new IllegalArgumentException("Allocation size cannot be negative.");
        long offset = nextAvailableOffset;
        nextAvailableOffset += size;
        return offset;
    }

    // --- PUBLIC ALLOCATION METHODS ---

    /** Allocates a generic block of space. */
    public long allocateGenericBlock(long size) {
        return allocateBlock(size);
    }

    /** Allocates space for a Message Continuation block. */
    public long allocateNextMessageContinuation(long continuationSize) {
        if (continuationSize <= 0L) throw new IllegalArgumentException("Continuation size must be positive.");
        return allocateBlock(continuationSize);
    }

    /** Allocates space for Dataset raw data. */
    public long allocateDataObjectData(long dataSize) {
        if (dataSize < 0L) throw new IllegalArgumentException("Data size cannot be negative.");
        return allocateBlock(dataSize);
    }

    /** Allocates space for a new SNOD block and tracks its offset. */
    public long allocateNextSnodStorage() {
        long allocationSize = SNOD_STORAGE_SIZE;
        if (allocationSize <= 0L) throw new IllegalStateException("SNOD storage size is not valid: " + allocationSize);
        long offset = allocateBlock(allocationSize);
        snodAllocationOffsets.add(offset);
        return offset;
    }

    /** Expands storage for Local Heap contents, allocating a new block. */
    public long expandLocalHeapContents() {
        long oldSize = this.currentLocalHeapContentsSize;
        if (oldSize <= 0L) throw new IllegalStateException("Cannot expand heap with non-positive current tracked size: " + oldSize);
        long newSize = oldSize * 2L;
        long newOffset = allocateBlock(newSize);
        this.currentLocalHeapContentsOffset = newOffset;
        this.currentLocalHeapContentsSize = newSize;
        return newOffset;
    }

    /** Allocates space for a generic Object Header block. */
    public long allocateNextObjectHeader(long totalHeaderSize) {
        if (totalHeaderSize <= 0L) throw new IllegalArgumentException("Object Header total size must be positive.");
        return allocateBlock(totalHeaderSize);
    }

    /** Allocates the first Global Heap block. */
    public long allocateFirstGlobalHeapBlock() {
        if (globalHeapOffset != -1L) throw new IllegalStateException("Global heap already allocated at " + globalHeapOffset);
        long offset = allocateBlock(GLOBAL_HEAP_BLOCK_SIZE);
        this.globalHeapOffset = offset;
        return offset;
    }

    /** Allocates a subsequent Global Heap block. */
    public long allocateNextGlobalHeapBlock() {
        return allocateBlock(GLOBAL_HEAP_BLOCK_SIZE);
    }

    /**
     * Checks whether a global heap block has been allocated.
     *
     * @return true if a global heap block has been allocated, false otherwise.
     */
    public boolean hasGlobalHeapAllocation() {
        return globalHeapOffset != -1L;
    }

    // --- Dataset Specific Allocation Methods ---

    /** Allocates initial space for a dataset's object header. */
    public DatasetAllocationInfo allocateDatasetStorage(String datasetName) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (datasetName.isEmpty()) throw new IllegalArgumentException("Dataset name cannot be empty");
        if (datasetAllocations.containsKey(datasetName)) throw new IllegalStateException("Dataset '" + datasetName + "' already allocated.");
        long headerAllocSize = DEFAULT_DATASET_HEADER_ALLOCATION_SIZE;
        long headerOffset;
        if (datasetAllocations.isEmpty()) {
            // For the first dataset, place header at 800 (after local heap contents)
            headerOffset = initialLocalHeapContentsOffset + initialLocalHeapContentsSize; // 712 + 88 = 800
            nextAvailableOffset = headerOffset + headerAllocSize; // 800 + 272 = 1072
        } else {
            // For subsequent datasets, use nextAvailableOffset and recalculate
            headerOffset = allocateBlock(headerAllocSize);
            recalculateLayout(); // Adjust layout for additional datasets
        }
        DatasetAllocationInfo info = new DatasetAllocationInfo(headerOffset, headerAllocSize);
        datasetAllocations.put(datasetName, info);
        return info;
    }

    /** Increases header size and triggers layout recalculation (before data lock). */
    public void increaseHeaderAllocation(String datasetName, long newTotalHeaderSize) {
        if (dataBlocksAllocated) throw new IllegalStateException("Cannot increase header allocation after data blocks have been allocated.");
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        DatasetAllocationInfo targetInfo = datasetAllocations.get(datasetName);
        if (targetInfo == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (newTotalHeaderSize <= targetInfo.getHeaderSize()) throw new IllegalArgumentException("New size must be greater than current size.");
        targetInfo.setHeaderSize(newTotalHeaderSize);
        recalculateLayout();
    }

    /** Allocates the data block, updates info, and sets the global header resize lock. */
    public long allocateAndSetDataBlock(String datasetName, long dataSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (dataSize < 0L) throw new IllegalArgumentException("Data size cannot be negative.");
        DatasetAllocationInfo info = datasetAllocations.get(datasetName);
        if (info == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (info.getDataOffset() != -1L || info.getDataSize() != -1L) throw new IllegalStateException("Data block for '" + datasetName + "' already allocated.");

        // Ensure data block starts at or after MIN_DATA_OFFSET_THRESHOLD
        if (nextAvailableOffset < MIN_DATA_OFFSET_THRESHOLD) {
            nextAvailableOffset = MIN_DATA_OFFSET_THRESHOLD;
        }
        long dataOffset = allocateBlock(dataSize);
        info.setDataOffset(dataOffset);
        info.setDataSize(dataSize);
        if (!this.dataBlocksAllocated) {
            this.dataBlocksAllocated = true;
        }
        return dataOffset;
    }

    /** Allocates the continuation block and updates info. */
    public long allocateAndSetContinuationBlock(String datasetName, long continuationSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (continuationSize <= 0L) throw new IllegalArgumentException("Continuation size must be positive.");
        DatasetAllocationInfo info = datasetAllocations.get(datasetName);
        if (info == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (info.getContinuationSize() != -1L) throw new IllegalStateException("Continuation block for '" + datasetName + "' already allocated/set.");
        long continuationOffset = allocateBlock(continuationSize);
        info.setContinuationOffset(continuationOffset);
        info.setContinuationSize(continuationSize);
        return continuationOffset;
    }

    // --- Getters ---
    public DatasetAllocationInfo getDatasetAllocationInfo(String datasetName) { return datasetAllocations.get(datasetName); }
    public Map<String, DatasetAllocationInfo> getAllDatasetAllocations() { return Collections.unmodifiableMap(new LinkedHashMap<>(datasetAllocations)); }
    public long getEndOfFileOffset() { return nextAvailableOffset; }
    public long getSuperblockSize() { return SUPERBLOCK_SIZE; }
    public long getGlobalHeapOffset() { return globalHeapOffset; } // Returns offset of *first* allocated GH
    public long getCurrentLocalHeapContentsOffset() { return currentLocalHeapContentsOffset; } // Offset of *current* LH Contents
    public long getCurrentLocalHeapContentsSize() { return currentLocalHeapContentsSize; }     // Size of *current* LH Contents
    public boolean isDataBlocksAllocated() { return dataBlocksAllocated; }
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

    public long getSnodOffset() { return snodOffset; } // Root group SNOD offset

    // --- NEW METHODS FOR ROOT GROUP ---
    /**
     * Gets the total size required for the root group's contiguous blocks:
     * object header prefix, B-tree, local heap header, and initial local heap contents.
     * @return The total size in bytes.
     */
    public long getRootGroupSize() {
        return SETUP_OBJECT_HEADER_PREFIX_SIZE + btreeTotalSize + SETUP_LOCAL_HEAP_HEADER_SIZE + initialLocalHeapContentsSize;
    }

    /**
     * Gets the starting offset of the root group's contiguous blocks.
     * Currently identical to getObjectHeaderPrefixOffset(), but provided for clarity
     * and future-proofing when multiple groups are supported.
     * @return The offset of the root group's first block (object header prefix).
     */
    public long getRootGroupOffset() {
        return objectHeaderPrefixOffset;
    }
    // --- END NEW METHODS ---

    // --- NEW METHODS FOR SNOD TRACKING ---
    /**
     * Gets an unmodifiable list of all SNOD allocation offsets.
     * Each SNOD block has a fixed size defined by SNOD_STORAGE_SIZE.
     * @return List of offsets for each SNOD block allocated.
     */
    public List<Long> getAllSnodAllocationOffsets() {
        return Collections.unmodifiableList(new ArrayList<>(snodAllocationOffsets));
    }
    // --- END NEW METHODS ---

    // --- Utility / Reset ---
    public void reset() {
        this.nextAvailableOffset = 0L;
        calculateInitialLayout();
    }

    /**
     * Prints all allocated blocks in order of their offsets, including their names, sizes,
     * and offsets in both decimal and hexadecimal format.
     * Useful for debugging the layout of the HDF file allocation.
     */
    public void printBlocks() {
        System.out.println("=== HDF File Allocation Layout ===");
        System.out.println("Current End of File Offset: " + nextAvailableOffset);
        System.out.println("Data Blocks Allocated: " + dataBlocksAllocated);
        System.out.println("----------------------------------");

        // Create a list to store all blocks with their details
        List<BlockInfo> blocks = new ArrayList<>();

        // Add fixed setup blocks
        blocks.add(new BlockInfo("Superblock", superblockOffset, SUPERBLOCK_SIZE));
        blocks.add(new BlockInfo("Object Header Prefix", objectHeaderPrefixOffset, SETUP_OBJECT_HEADER_PREFIX_SIZE));
        blocks.add(new BlockInfo("B-tree (Node + Storage)", btreeOffset, btreeTotalSize));
        blocks.add(new BlockInfo("Local Heap Header", localHeapHeaderOffset, SETUP_LOCAL_HEAP_HEADER_SIZE));
        blocks.add(new BlockInfo("Initial Local Heap Contents", initialLocalHeapContentsOffset, initialLocalHeapContentsSize));

        // Add dataset headers
        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            String name = entry.getKey();
            DatasetAllocationInfo info = entry.getValue();
            blocks.add(new BlockInfo("Dataset Header (" + name + ")", info.getHeaderOffset(), info.getHeaderSize()));
        }

        // Add SNOD blocks
        for (int i = 0; i < snodAllocationOffsets.size(); i++) {
            blocks.add(new BlockInfo("SNOD Block " + (i + 1), snodAllocationOffsets.get(i), SNOD_STORAGE_SIZE));
        }

        // Add current local heap contents (if expanded beyond initial)
        if (currentLocalHeapContentsOffset != initialLocalHeapContentsOffset) {
            blocks.add(new BlockInfo("Expanded Local Heap Contents", currentLocalHeapContentsOffset, currentLocalHeapContentsSize));
        }

        // Add global heap block (if allocated)
        if (globalHeapOffset != -1L) {
            blocks.add(new BlockInfo("Global Heap Block", globalHeapOffset, GLOBAL_HEAP_BLOCK_SIZE));
        }

        // Add dataset continuations
        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            String name = entry.getKey();
            DatasetAllocationInfo info = entry.getValue();
            if (info.getContinuationOffset() != -1L) {
                blocks.add(new BlockInfo("Continuation (" + name + ")", info.getContinuationOffset(), info.getContinuationSize()));
            }
        }

        // Add dataset data blocks
        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            String name = entry.getKey();
            DatasetAllocationInfo info = entry.getValue();
            if (info.getDataOffset() != -1L) {
                blocks.add(new BlockInfo("Data Block (" + name + ")", info.getDataOffset(), info.getDataSize()));
            }
        }

        // Sort blocks by offset
        blocks.sort(Comparator.comparingLong(BlockInfo::getOffset));

        // Print header
        System.out.println("Offset (Dec) | Offset (Hex) | Size     | Name");

        // Print all blocks
        for (BlockInfo block : blocks) {
            String hexOffset = String.format("0x%08X", block.getOffset());
            System.out.printf("%-12d | %-12s | %-8d | %s%n",
                    block.getOffset(), hexOffset, block.getSize(), block.getName());
        }

        System.out.println("==================================");
    }

    /**
     * Helper class to store block information for sorting and printing.
     */
    private static class BlockInfo {
        private final String name;
        private final long offset;
        private final long size;

        public BlockInfo(String name, long offset, long size) {
            this.name = name;
            this.offset = offset;
            this.size = size;
        }

        public String getName() { return name; }
        public long getOffset() { return offset; }
        public long getSize() { return size; }
    }
}