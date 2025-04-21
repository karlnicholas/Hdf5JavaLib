package org.hdf5javalib.file;

import lombok.Getter;

import java.util.*;

/**
 * Class managing the allocation layout (offsets and sizes) via recalculation.
 * Responsible for all allocation actions and updating DatasetAllocationInfo state.
 * Supports increasing dataset header size with layout recalculation.
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
    private static final long DEFAULT_DATASET_HEADER_ALLOCATION_SIZE = DATA_OBJECT_HEADER_MESSAGE_SIZE + 256L;
    private static final long MIN_DATA_OFFSET_THRESHOLD = 2048L;
    @Getter
    private static final long SNOD_STORAGE_SIZE = SETUP_SNOD_V1_HEADER_SIZE + (DEFAULT_SETUP_SNOD_ENTRY_COUNT * SETUP_SNOD_V1_ENTRY_SIZE);

    // --- Storage for Multiple Dataset Allocations ---
    private final Map<String, DatasetAllocationInfo> datasetAllocations = new LinkedHashMap<>();

    // --- Storage for SNOD Allocations (Offsets Only) ---
    private final List<Long> snodAllocationOffsets = new ArrayList<>();

    // --- Storage for Global Heap Block Sizes ---
    private final Map<Long, Long> globalHeapBlockSizes = new HashMap<>();
    private int globalHeapBlockCount = 0;
    private long secondGlobalHeapOffset = -1L;

    // --- Fixed Setup Block Offsets (Calculated once) ---
    private final long superblockOffset = SUPERBLOCK_OFFSET;
    private long objectHeaderPrefixOffset;
    private long btreeOffset;
    private long localHeapHeaderOffset;
    private long initialLocalHeapContentsOffset;
    private long snodOffset;

    // --- Fixed Setup Block Sizes ---
    private final long btreeTotalSize;
    private final long initialLocalHeapContentsSize = SETUP_LOCAL_HEAP_CONTENTS_SIZE;

    // --- Tracking for Dynamically Allocated/Sized Components ---
    private long currentLocalHeapContentsOffset;
    private long currentLocalHeapContentsSize;
    private long globalHeapOffset = -1L;

    // --- Allocation Tracking ---
    private long nextAvailableOffset;

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
        localHeapHeaderOffset = currentOffset; currentOffset += SETUP_LOCAL_HEAP_HEADER_SIZE;
        initialLocalHeapContentsOffset = currentOffset; currentOffset += initialLocalHeapContentsSize;
        currentLocalHeapContentsOffset = initialLocalHeapContentsOffset;
        currentLocalHeapContentsSize = initialLocalHeapContentsSize;
        globalHeapOffset = -1L;
        secondGlobalHeapOffset = -1L;
        this.datasetAllocations.clear();
        this.snodAllocationOffsets.clear();
        this.globalHeapBlockSizes.clear();
        this.globalHeapBlockCount = 0;
        this.nextAvailableOffset = currentOffset;
    }

    /** Recalculates layout based on current sizes. */
    private void recalculateLayout() {
        long currentOffset = initialLocalHeapContentsOffset + initialLocalHeapContentsSize;

        // Maintain order of dataset headers and SNODs as allocated
        List<Object> allocationOrder = new ArrayList<>();
        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            allocationOrder.add(entry.getValue());
        }
        for (Long snodOffset : snodAllocationOffsets) {
            allocationOrder.add(snodOffset);
        }

        // Assign offsets in order
        snodOffset = -1L;
        snodAllocationOffsets.clear();
        for (Object alloc : allocationOrder) {
            if (alloc instanceof DatasetAllocationInfo) {
                DatasetAllocationInfo info = (DatasetAllocationInfo) alloc;
                info.setHeaderOffset(currentOffset);
                currentOffset += info.getHeaderSize();
            } else if (alloc instanceof Long) {
                Long snod = (Long) alloc;
                snodAllocationOffsets.add(currentOffset);
                if (snodOffset == -1L) {
                    snodOffset = currentOffset;
                }
                currentOffset += SNOD_STORAGE_SIZE;
            }
        }

        // Place first allocated global heap block, if any
        if (globalHeapOffset != -1L) {
            globalHeapOffset = currentOffset;
            currentOffset += globalHeapBlockSizes.getOrDefault(globalHeapOffset, GLOBAL_HEAP_BLOCK_SIZE);
        }
        // Place second global heap block, if any
        if (secondGlobalHeapOffset != -1L) {
            secondGlobalHeapOffset = currentOffset;
            currentOffset += globalHeapBlockSizes.getOrDefault(secondGlobalHeapOffset, GLOBAL_HEAP_BLOCK_SIZE);
        }
        // Place expanded local heap contents block, if it’s not the initial one
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

    private String getDatasetName(DatasetAllocationInfo targetInfo) {
        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            if (entry.getValue() == targetInfo) return entry.getKey();
        }
        return "[Unknown Dataset]";
    }

    private long allocateBlock(long size) {
        if (size < 0L) throw new IllegalArgumentException("Allocation size cannot be negative.");
        long offset = nextAvailableOffset;
        nextAvailableOffset += size;
        return offset;
    }

    public long allocateGenericBlock(long size) {
        return allocateBlock(size);
    }

    public long allocateNextMessageContinuation(long continuationSize) {
        if (continuationSize <= 0L) throw new IllegalArgumentException("Continuation size must be positive.");
        return allocateBlock(continuationSize);
    }

    public long allocateDataObjectData(long dataSize) {
        if (dataSize < 0L) throw new IllegalArgumentException("Data size cannot be negative.");
        return allocateBlock(dataSize);
    }

    public long allocateNextSnodStorage() {
System.out.println("allocateNextSnodStorage");
        long allocationSize = SNOD_STORAGE_SIZE;
        if (allocationSize <= 0L) throw new IllegalStateException("SNOD storage size is not valid: " + allocationSize);
        long offset = nextAvailableOffset;
        snodAllocationOffsets.add(offset);
        nextAvailableOffset += allocationSize;
        recalculateLayout();
        return offset;
    }

    public long expandLocalHeapContents() {
        long oldSize = this.currentLocalHeapContentsSize;
        if (oldSize <= 0L) throw new IllegalStateException("Cannot expand heap with non-positive current tracked size: " + oldSize);
        long newSize = oldSize * 2L;
        long newOffset = allocateBlock(newSize);
        this.currentLocalHeapContentsOffset = newOffset;
        this.currentLocalHeapContentsSize = newSize;
        return newOffset;
    }

    public long expandGlobalHeapBlock() {
        if (secondGlobalHeapOffset == -1L) {
            throw new IllegalStateException("Second global heap block not yet allocated.");
        }
        long oldSize = globalHeapBlockSizes.getOrDefault(secondGlobalHeapOffset, GLOBAL_HEAP_BLOCK_SIZE);
        long newSize = oldSize * 2L;
        // Update size in place, assuming block is at end of file
        nextAvailableOffset = secondGlobalHeapOffset + newSize;
        globalHeapBlockSizes.put(secondGlobalHeapOffset, newSize);
        return secondGlobalHeapOffset;
    }

    public long allocateNextObjectHeader(long totalHeaderSize) {
        if (totalHeaderSize <= 0L) throw new IllegalArgumentException("Object Header total size must be positive.");
        return allocateBlock(totalHeaderSize);
    }

    public long allocateFirstGlobalHeapBlock() {
        if (globalHeapOffset != -1L) throw new IllegalStateException("Global heap already allocated at " + globalHeapOffset);
        globalHeapBlockCount = 1;
        long size = GLOBAL_HEAP_BLOCK_SIZE; // 4096 bytes for first block
        long offset = allocateBlock(size);
        globalHeapBlockSizes.put(offset, size);
        this.globalHeapOffset = offset;
        return offset;
    }

    public long allocateNextGlobalHeapBlock() {
        if (globalHeapBlockCount >= 2) {
            throw new IllegalStateException("Only two global heap blocks allowed.");
        }
        globalHeapBlockCount = 2;
        long size = GLOBAL_HEAP_BLOCK_SIZE; // 4096 bytes for second block
        long offset = allocateBlock(size);
        globalHeapBlockSizes.put(offset, size);
        this.secondGlobalHeapOffset = offset;
        return offset;
    }

    public long getGlobalHeapBlockSize(long offset) {
        return globalHeapBlockSizes.getOrDefault(offset, GLOBAL_HEAP_BLOCK_SIZE);
    }

    public boolean hasGlobalHeapAllocation() {
        return globalHeapOffset != -1L;
    }

    public DatasetAllocationInfo allocateDatasetStorage(String datasetName) {
System.out.println("allocateDatasetStorage '" + datasetName + "'");
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (datasetName.isEmpty()) throw new IllegalArgumentException("Dataset name cannot be empty");
        if (datasetAllocations.containsKey(datasetName)) throw new IllegalStateException("Dataset '" + datasetName + "' already allocated.");
        long headerAllocSize = DEFAULT_DATASET_HEADER_ALLOCATION_SIZE;
        long headerOffset = nextAvailableOffset;
        nextAvailableOffset += headerAllocSize;
        DatasetAllocationInfo info = new DatasetAllocationInfo(headerOffset, headerAllocSize);
        datasetAllocations.put(datasetName, info);
        recalculateLayout();
        return info;
    }

    public void increaseHeaderAllocation(String datasetName, long newTotalHeaderSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        DatasetAllocationInfo targetInfo = datasetAllocations.get(datasetName);
        if (targetInfo == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (newTotalHeaderSize <= targetInfo.getHeaderSize()) throw new IllegalArgumentException("New size must be greater than current size.");
        targetInfo.setHeaderSize(newTotalHeaderSize);
        recalculateLayout();
    }

    public long allocateAndSetDataBlock(String datasetName, long dataSize) {
System.out.println("allocateAndSetDataBlock '" + datasetName + "' dataSize " + dataSize);
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (dataSize < 0L) throw new IllegalArgumentException("Data size cannot be negative.");
        DatasetAllocationInfo info = datasetAllocations.get(datasetName);
        if (info == null) throw new IllegalStateException("Dataset '" + datasetName + "' not found.");
        if (info.getDataOffset() != -1L || info.getDataSize() != -1L) throw new IllegalStateException("Data block for '" + datasetName + "' already allocated.");

        if (nextAvailableOffset < MIN_DATA_OFFSET_THRESHOLD) {
            nextAvailableOffset = MIN_DATA_OFFSET_THRESHOLD;
        }
        long dataOffset = allocateBlock(dataSize);
        info.setDataOffset(dataOffset);
        info.setDataSize(dataSize);
        return dataOffset;
    }

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

    public DatasetAllocationInfo getDatasetAllocationInfo(String datasetName) { return datasetAllocations.get(datasetName); }
    public Map<String, DatasetAllocationInfo> getAllDatasetAllocations() { return Collections.unmodifiableMap(new LinkedHashMap<>(datasetAllocations)); }
    public long getEndOfFileOffset() { return nextAvailableOffset; }
    public long getSuperblockSize() { return SUPERBLOCK_SIZE; }
    public long getGlobalHeapOffset() { return globalHeapOffset; }
    public long getCurrentLocalHeapContentsOffset() { return currentLocalHeapContentsOffset; }
    public long getCurrentLocalHeapContentsSize() { return currentLocalHeapContentsSize; }
    public boolean isDataBlocksAllocated() { return false; }
    public long getObjectHeaderPrefixOffset() { return objectHeaderPrefixOffset; }
    public long getBtreeOffset() { return btreeOffset; }

    /**
     * Gets the offset of the Local Heap *Header* block allocated during initial setup.
     * @return The offset of the setup Local Heap Header.
     */
    public long getLocalHeapOffset() {
        return localHeapHeaderOffset;
    }

    public long getSnodOffset() { return snodOffset; }

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

    /**
     * Gets an unmodifiable list of all SNOD allocation offsets.
     * Each SNOD block has a fixed size defined by SNOD_STORAGE_SIZE.
     * @return List of offsets for each SNOD block allocated.
     */
    public List<Long> getAllSnodAllocationOffsets() {
        return Collections.unmodifiableList(new ArrayList<>(snodAllocationOffsets));
    }

    public void reset() {
        this.nextAvailableOffset = 0L;
        calculateInitialLayout();
    }

    public void printBlocks() {
        System.out.println("=== HDF File Allocation Layout ===");
        System.out.println("Current End of File Offset: " + nextAvailableOffset);
        System.out.println("----------------------------------");

        List<BlockInfo> blocks = new ArrayList<>();
        blocks.add(new BlockInfo("Superblock", superblockOffset, SUPERBLOCK_SIZE));
        blocks.add(new BlockInfo("Object Header Prefix", objectHeaderPrefixOffset, SETUP_OBJECT_HEADER_PREFIX_SIZE));
        blocks.add(new BlockInfo("B-tree (Node + Storage)", btreeOffset, btreeTotalSize));
        blocks.add(new BlockInfo("Local Heap Header", localHeapHeaderOffset, SETUP_LOCAL_HEAP_HEADER_SIZE));
        blocks.add(new BlockInfo("Initial Local Heap Contents", initialLocalHeapContentsOffset, initialLocalHeapContentsSize));

        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            String name = entry.getKey();
            DatasetAllocationInfo info = entry.getValue();
            blocks.add(new BlockInfo("Dataset Header (" + name + ")", info.getHeaderOffset(), info.getHeaderSize()));
        }

        for (int i = 0; i < snodAllocationOffsets.size(); i++) {
            blocks.add(new BlockInfo("SNOD Block " + (i + 1), snodAllocationOffsets.get(i), SNOD_STORAGE_SIZE));
        }

        if (currentLocalHeapContentsOffset != initialLocalHeapContentsOffset) {
            blocks.add(new BlockInfo("Expanded Local Heap Contents", currentLocalHeapContentsOffset, currentLocalHeapContentsSize));
        }

        int blockNumber = 1;
        for (Map.Entry<Long, Long> entry : globalHeapBlockSizes.entrySet()) {
            blocks.add(new BlockInfo("Global Heap Block " + blockNumber, entry.getKey(), entry.getValue()));
            blockNumber++;
        }

        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            String name = entry.getKey();
            DatasetAllocationInfo info = entry.getValue();
            if (info.getContinuationOffset() != -1L) {
                blocks.add(new BlockInfo("Continuation (" + name + ")", info.getContinuationOffset(), info.getContinuationSize()));
            }
        }

        for (Map.Entry<String, DatasetAllocationInfo> entry : datasetAllocations.entrySet()) {
            String name = entry.getKey();
            DatasetAllocationInfo info = entry.getValue();
            if (info.getDataOffset() != -1L) {
                blocks.add(new BlockInfo("Data Block (" + name + ")", info.getDataOffset(), info.getDataSize()));
            }
        }

        blocks.sort(Comparator.comparingLong(BlockInfo::getOffset));

        System.out.println("Offset (Dec) | Offset (Hex) | Size     | Name");
        for (BlockInfo block : blocks) {
            String hexOffset = String.format("0x%08X", block.getOffset());
            System.out.printf("%-12d | %-12s | %-8d | %s%n",
                    block.getOffset(), hexOffset, block.getSize(), block.getName());
        }

        System.out.println("==================================");
    }

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