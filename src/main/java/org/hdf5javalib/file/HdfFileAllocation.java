package org.hdf5javalib.file; // Adjust package if needed

import lombok.Getter;

/**
 * Singleton class managing the allocation layout (offsets and sizes) of structures
 * within an HDF5 file during the writing process. All sizes are represented as long.
 * (Javadoc unchanged)
 */
@Getter
public final class HdfFileAllocation {

    /**
     * -- GETTER --
     *  Returns the single instance (unchanged)
     */
    // --- Singleton Instance ---
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
    private static final long DATA_OBJECT_HEADER_MESSAGE_SIZE = 16L; // v1 minimum prefix
    private static final long SETUP_SNOD_V1_HEADER_SIZE = 8L;
    private static final long SETUP_SNOD_V1_ENTRY_SIZE = 32L;
    private static final long DEFAULT_SETUP_SNOD_ENTRY_COUNT = 10L;
    private static final long MIN_DATA_OFFSET_THRESHOLD = 2048L;
    /** Public constant defining the standard size for allocated Global Heap blocks. */
    public static final long GLOBAL_HEAP_BLOCK_SIZE = 4096L;


    // --- Calculated Block Offsets (Setup Layout) ---
    private final long superblockOffset = SUPERBLOCK_OFFSET;
    private long objectHeaderPrefixOffset;
    private long btreeOffset;
    private long localHeapOffset;
    private long localHeapContentsOffset;
    private long snodOffset;
    private long dataObjectHeaderOffset;
    private long dataObjectHeaderContinuationOffset;
    private long dataObjectDataOffset;

    // --- Sizes (long) ---
    private final long btreeStorageSize = SETUP_BTREE_STORAGE_SIZE;
    private final long localHeapContentsSize = SETUP_LOCAL_HEAP_CONTENTS_SIZE;
    private final long snodStorageSize = SETUP_SNOD_V1_HEADER_SIZE + (DEFAULT_SETUP_SNOD_ENTRY_COUNT * SETUP_SNOD_V1_ENTRY_SIZE);
    private long dataObjectHeaderSize = 256L;
    private long dataObjectHeaderContinuationSize = 0L;

    // --- Tracking for Active/Expandable Structures ---
    private long currentLocalHeapContentsOffset;
    private long currentLocalHeapContentsSize;

    // --- Allocation Tracking ---
    private long nextAvailableOffset;

    // --- Global Heap Tracking ---
    private long globalHeapOffset = -1L;


    /**
     * Private Constructor (unchanged)
     */
    private HdfFileAllocation() {
        calculateSetupLayout();
    }

    /** Calculates the offsets of the structures within the setup metadata block. */
    private void calculateSetupLayout() {
        long currentOffset = SUPERBLOCK_OFFSET;

        currentOffset += SUPERBLOCK_SIZE;
        objectHeaderPrefixOffset = currentOffset;

        currentOffset += SETUP_OBJECT_HEADER_PREFIX_SIZE;
        btreeOffset = currentOffset;

        currentOffset += SETUP_BTREE_NODE_SIZE + btreeStorageSize;
        localHeapOffset = currentOffset;

        currentOffset += SETUP_LOCAL_HEAP_HEADER_SIZE;
        localHeapContentsOffset = currentOffset;

        currentOffset += localHeapContentsSize;
        dataObjectHeaderOffset = currentOffset;

        currentOffset += DATA_OBJECT_HEADER_MESSAGE_SIZE + dataObjectHeaderSize;
        snodOffset = currentOffset;

        currentOffset += snodStorageSize;
        if (dataObjectHeaderContinuationSize > 0L) {
            dataObjectHeaderContinuationOffset = currentOffset;
            currentOffset += dataObjectHeaderContinuationSize;
        } else {
            dataObjectHeaderContinuationOffset = -1L;
        }
        long endOfSetupBlock = currentOffset;

        currentLocalHeapContentsOffset = localHeapContentsOffset;
        currentLocalHeapContentsSize = localHeapContentsSize;

        if (this.nextAvailableOffset > 0L) { // If called after initial construction
            this.nextAvailableOffset = Math.max(this.nextAvailableOffset, endOfSetupBlock);
        } else { // First time (during construction)
            this.nextAvailableOffset = endOfSetupBlock;
        }

        dataObjectDataOffset = Math.max(MIN_DATA_OFFSET_THRESHOLD, endOfSetupBlock);
        globalHeapOffset = -1L; // Ensure it starts as uncomputed/unallocated
    }

    // --- Methods to Modify Pre-calculated Layout ---

    /**
     * Sets the total size for the first Data Object header block calculated
     * during setup and recalculates the layout. Thread-safe.
     */
    public synchronized void setDataObjectHeaderSize(long totalHeaderSize) {
        this.dataObjectHeaderSize = totalHeaderSize;
        calculateSetupLayout();
    }

    /**
     * Sets the size for the continuation block associated with the first Data Object header
     * calculated during setup and recalculates the layout. Thread-safe.
     */
    public synchronized void setDataObjectHeaderContinuationSize(long continuationSize) {
        if (continuationSize < 0L) {
            throw new IllegalArgumentException("Message continuation size cannot be negative.");
        }
        this.dataObjectHeaderContinuationSize = continuationSize;
        calculateSetupLayout();
    }

    /**
     * Sets the total size for the first Data Object header and the size for its
     * associated message continuation block, then recalculates the setup layout once. Thread-safe.
     */
    public synchronized void setDataObjectHeaderAndContinuationSizes(long totalHeaderSize, long continuationSize) {
        if (continuationSize < 0L) {
            throw new IllegalArgumentException("Message continuation size cannot be negative.");
        }
        this.dataObjectHeaderSize = totalHeaderSize;
        this.dataObjectHeaderContinuationSize = continuationSize;
        calculateSetupLayout();
    }


    // --- Methods for Dynamic Allocation (Append Strategy) ---

    /**
     * Allocates a generic block of space at the next available offset. Thread-safe.
     */
    public synchronized long allocateGenericBlock(long size) {
        if (size <= 0L) {
            throw new IllegalArgumentException("Allocation size must be positive.");
        }
        long allocationOffset = nextAvailableOffset;
        nextAvailableOffset += size;
        return allocationOffset;
    }

    /**
     * Allocates space for a new SNOD block using the size determined during setup. Thread-safe.
     */
    public synchronized long allocateNextSnodStorage() {
        long allocationSize = this.snodStorageSize;
        if (allocationSize <= 0L) {
            throw new IllegalStateException("Setup SNOD storage size is not valid.");
        }
        return allocateGenericBlock(allocationSize);
    }

    /**
     * Allocates space for a new Object Header block (prefix + messages). Thread-safe.
     */
    public synchronized long allocateNextObjectHeader(long messageStorageSize) {
        if (messageStorageSize < 0L) {
            throw new IllegalArgumentException("Message storage size cannot be negative.");
        }
        return allocateGenericBlock(messageStorageSize);
    }

    /**
     * Allocates space for a new Message Continuation block. Thread-safe.
     */
    public synchronized long allocateNextMessageContinuation(long continuationSize) {
        return allocateGenericBlock(continuationSize);
    }

    /**
     * Allocates space specifically for Dataset DataObjectData. Thread-safe.
     */
    public synchronized long allocateDataObjectData(long dataSize) {
        return allocateGenericBlock(dataSize);
    }

    /**
     * Expands storage for Local Heap contents, allocating a new block. Thread-safe.
     */
    public synchronized long expandLocalHeapContents() {
        long oldSize = this.currentLocalHeapContentsSize;
        if (oldSize <= 0L) {
            throw new IllegalStateException("Cannot expand heap with non-positive current size.");
        }
        long newSize = oldSize * 2L;
        long newOffset = allocateGenericBlock(newSize);
        this.currentLocalHeapContentsOffset = newOffset;
        this.currentLocalHeapContentsSize = newSize;
        return newOffset;
    }


    // --- Global Heap ---

    /**
     * Computes the offset for and allocates the *first* Global Heap block based on the
     * Data Object Data layout, using the standard GLOBAL_HEAP_BLOCK_SIZE. Sets this as the currently active global heap.
     * Updates the next available offset accordingly. Thread-safe.
     */
    public synchronized long allocateFirstGlobalHeapBlock(long totalDataSegmentSize) {
        if (globalHeapOffset != -1L) {
            throw new IllegalStateException("First Global Heap block has already been allocated or computed at offset " + globalHeapOffset);
        }
        if (totalDataSegmentSize < 0L) {
            throw new IllegalArgumentException("Total data segment size cannot be negative.");
        }
        if (dataObjectDataOffset < 0L ) {
            throw new IllegalStateException("Data object data offset invalid.");
        }

        long firstHeapStartOffset = dataObjectDataOffset + totalDataSegmentSize;

        // Use public constant

        this.globalHeapOffset = allocateGenericBlock(GLOBAL_HEAP_BLOCK_SIZE);

        return this.globalHeapOffset;
    }

    /**
     * Allocates a new Global Heap block using the standard GLOBAL_HEAP_BLOCK_SIZE
     * at the next available file offset and sets it as the *currently active* global heap.
     * Thread-safe.
     */
    public synchronized long allocateNextGlobalHeapBlock() {
        // Use public constant
        this.globalHeapOffset = allocateGenericBlock(GLOBAL_HEAP_BLOCK_SIZE);
        return this.globalHeapOffset;
    }


    /**
     * Gets the offset of the *currently active* Global Heap block.
     * Returns -1 if the first block has not yet been allocated via allocateFirstGlobalHeapBlock().
     * Thread-safe for reading.
     */
    public synchronized long getGlobalHeapOffset() {
        return globalHeapOffset;
    }

    // --- Getters ---
    /**
     * Gets the total size allocated or reserved so far. Thread-safe for reading.
     */
    public long getTotalAllocatedSize() {
        return nextAvailableOffset;
    }

}