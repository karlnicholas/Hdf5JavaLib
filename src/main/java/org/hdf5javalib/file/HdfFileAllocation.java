package org.hdf5javalib.file; // Adjust package if needed

import lombok.Getter;

/**
 * Manages the allocation layout (offsets and sizes) of structures within an HDF5 file
 * during the writing process. All sizes are represented as long.
 * <p>
 * Calculates a layout for core structures (Superblock, first Group's metadata)
 * and the first Data Object Header immediately following them. Provides methods to allocate
 * space for dynamically added structures by appending them to the end of the
 * currently allocated space.
 */
@Getter
public class HdfFileAllocation {

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

    // --- Calculated Block Offsets (Setup Layout) ---
    private final long superblockOffset = SUPERBLOCK_OFFSET;
    private long objectHeaderPrefixOffset;
    private long btreeOffset;
    private long localHeapOffset;
    private long localHeapContentsOffset;
    private long snodOffset;
    /** Offset for the Data Object Header block calculated immediately following the setup SNOD block. */
    private long dataObjectHeaderOffset; // Renamed from dataObjectHeadOffset
    private long dataObjectHeaderContinuationOffset;
    private long dataObjectDataOffset;

    // --- Sizes (long) ---
    private final long btreeStorageSize = SETUP_BTREE_STORAGE_SIZE;
    private final long localHeapContentsSize = SETUP_LOCAL_HEAP_CONTENTS_SIZE;
    private final long snodStorageSize = SETUP_SNOD_V1_HEADER_SIZE + (DEFAULT_SETUP_SNOD_ENTRY_COUNT * SETUP_SNOD_V1_ENTRY_SIZE);
//    private long dataObjectHeaderSize = DATA_OBJECT_HEADER_MINIMUM_SIZE + 256L;
    private long dataObjectHeaderSize = 256L;
    private long dataObjectHeaderContinuationSize = 0L;

    // --- Tracking for Active/Expandable Structures ---
    private long currentLocalHeapContentsOffset;
    private long currentLocalHeapContentsSize;

    // --- Allocation Tracking ---
    private long nextAvailableOffset;

    // --- Other Calculated Offsets ---
    private long globalHeapOffset = -1L;


    /**
     * Constructor: Initializes the allocation manager and calculates the layout
     * of the setup metadata block.
     */
    public HdfFileAllocation() {
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

        if (nextAvailableOffset > 0L) {
            nextAvailableOffset = Math.max(nextAvailableOffset, endOfSetupBlock);
        } else {
            nextAvailableOffset = endOfSetupBlock;
        }

        dataObjectDataOffset = Math.max(MIN_DATA_OFFSET_THRESHOLD, endOfSetupBlock);
        globalHeapOffset = -1L;
    }

    // --- Methods to Modify Pre-calculated Layout ---

    /**
     * Sets the total size for the first Data Object header block calculated
     * during setup and recalculates the layout.
     * @param totalHeaderSize The total required size in bytes.
     */
    public void setDataObjectHeaderSize(long totalHeaderSize) {
        this.dataObjectHeaderSize = totalHeaderSize;
        calculateSetupLayout();
    }

    /**
     * Sets the size for the continuation block associated with the first Data Object header
     * calculated during setup and recalculates the layout.
     * @param continuationSize The required size in bytes (must be non-negative).
     */
    public void setDataObjectHeaderContinuationSize(long continuationSize) {
        if (continuationSize < 0L) {
            throw new IllegalArgumentException("Message continuation size cannot be negative.");
        }
        this.dataObjectHeaderContinuationSize = continuationSize;
        calculateSetupLayout();
    }

    /**
     * Sets the total size for the first Data Object header and the size for its
     * associated message continuation block, then recalculates the setup layout once.
     * @param totalHeaderSize The total required header size in bytes.
     * @param continuationSize The required continuation size in bytes (must be non-negative).
     */
    public void setDataObjectHeaderAndContinuationSizes(long totalHeaderSize, long continuationSize) {
        if (continuationSize < 0L) {
            throw new IllegalArgumentException("Message continuation size cannot be negative.");
        }
        this.dataObjectHeaderSize = totalHeaderSize;
        this.dataObjectHeaderContinuationSize = continuationSize;
        calculateSetupLayout();
    }

    // --- Methods for Dynamic Allocation (Append Strategy) ---

    /** Allocates a generic block of space at the next available offset. */
    public long allocateGenericBlock(long size) {
        if (size <= 0L) { throw new IllegalArgumentException("Allocation size must be positive."); }
        long allocationOffset = nextAvailableOffset;
        nextAvailableOffset += size;
        return allocationOffset;
    }

    /** Allocates space for a new SNOD block using the size determined during setup. */
    public long allocateNextSnodStorage() {
        long allocationSize = this.snodStorageSize;
        if (allocationSize <= 0L) { throw new IllegalStateException("Setup SNOD storage size is not valid."); }
        return allocateGenericBlock(allocationSize);
    }

    /** Allocates space for a new Object Header block (prefix + messages). */
    public long allocateNextObjectHeader(long messageStorageSize) {
        if (messageStorageSize < 0L) { throw new IllegalArgumentException("Message storage size cannot be negative."); }
        return allocateGenericBlock(messageStorageSize);
    }

    /** Allocates space for a new Message Continuation block. */
    public long allocateNextMessageContinuation(long continuationSize) {
        return allocateGenericBlock(continuationSize);
    }

    /** Allocates space specifically for Dataset DataObjectData. */
    public long allocateDataObjectData(long dataSize) {
        return allocateGenericBlock(dataSize);
    }

    /** Expands storage for Local Heap contents, allocating a new block. */
    public long expandLocalHeapContents() {
        long oldSize = this.currentLocalHeapContentsSize;
        if (oldSize <= 0L) { throw new IllegalStateException("Cannot expand heap with non-positive current size."); }
        long newSize = oldSize * 2L;
        long newOffset = allocateGenericBlock(newSize);
        this.currentLocalHeapContentsOffset = newOffset;
        this.currentLocalHeapContentsSize = newSize;
        return newOffset;
    }

    // --- Global Heap ---
    /** Computes the starting offset of the Global Heap. */
    public void computeGlobalHeapOffset(long totalDataSegmentSize) {
        if (totalDataSegmentSize < 0L) { throw new IllegalArgumentException("Total data segment size cannot be negative."); }
        if (dataObjectDataOffset < 0L ) { throw new IllegalStateException("Data object data offset invalid."); }
        this.globalHeapOffset = dataObjectDataOffset + totalDataSegmentSize;
        this.nextAvailableOffset = Math.max(this.nextAvailableOffset, this.globalHeapOffset);
    }

    // --- Getters ---
    // Lombok @Getter provides getters like: getObjectHeaderPrefixOffset(), getBtreeOffset(),
    // getDataObjectHeaderOffset(), getDataObjectHeaderContinuationOffset(), getDataObjectDataOffset(), etc.

    /** Gets the total size allocated or reserved so far. */
    public long getTotalAllocatedSize() { return nextAvailableOffset; }
}