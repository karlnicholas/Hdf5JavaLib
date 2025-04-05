package org.hdf5javalib.file; // Assuming same package or adjust as needed

import lombok.Getter;

/**
 * Manages the allocation layout (offsets and sizes) of structures within an HDF5 file
 * during the writing process. All sizes are represented as long.
 *
 * It calculates the initial layout for core structures (Superblock, Root Group metadata)
 * sequentially. It also provides methods to allocate space for dynamically added structures
 * (like new SNODs, Object Headers for datasets, Message Continuation blocks, expanded heaps)
 * by appending them to the end of the currently allocated space.
 */
@Getter
public class HdfFileAllocation {

    // --- Constants for Fixed Sizes (now long) ---
    public static final long SUPERBLOCK_OFFSET = 0L;
    public static final long SUPERBLOCK_SIZE = 96L;
    public static final long ROOT_OBJECT_HEADER_PREFIX_SIZE = 40L; // For root group OH
    public static final long BTREE_NODE_SIZE = 32L;
    public static final long BTREE_V1_STORAGE_SIZE = 512L; // Example default for root B-tree node store
    public static final long LOCAL_HEAP_HEADER_SIZE = 32L;
    public static final long LOCAL_HEAP_DEFAULT_CONTENTS_SIZE = 88L; // Example default for root local heap contents
    public static final long OBJECT_HEADER_MINIMUM_SIZE = 16L; // Minimum size for an object header (v1)
    public static final long SNOD_V1_HEADER_SIZE = 8L;
    public static final long SNOD_V1_ENTRY_SIZE = 32L;
    public static final long DEFAULT_INITIAL_SNOD_ENTRY_COUNT = 10L; // Initial allocated entries for root SNOD
    public static final long MIN_DATA_OFFSET = 2048L; // Minimum offset for actual data start


    // --- Initial Block Offsets (Calculated by recalcInitialLayout) ---
    private final long superblockOffset = SUPERBLOCK_OFFSET;
    private long rootObjectHeaderPrefixOffset; // OH for "/" group
    private long rootBtreeOffset;              // B-tree for "/" group links
    private long rootLocalHeapOffset;          // Local Heap Header for "/" group
    private long rootLocalHeapContentsOffset; // Offset of the *initial* storage block for LHeap contents
    private long initialSnodOffset;            // First SNOD, associated with root B-tree
    private long initialObjectHeaderOffset;    // e.g., for the first dataset/group created *after* root
    private long initialMessageContinuationOffset; // Associated with initialObjectHeaderOffset if needed
    private long dataOffset;                   // Start offset for raw dataset data chunks

    // --- Sizes (now long) ---
    private final long rootBtreeStorageSize = BTREE_V1_STORAGE_SIZE; // Assuming fixed for root for now
    /** Size (long) of the *initial* storage block allocated for the root Local Heap contents. Used for layout calculation. */
    private final long rootLocalHeapContentsSize = LOCAL_HEAP_DEFAULT_CONTENTS_SIZE;
    /** Size (long) of the storage allocated for the initial SNOD (header + entries). Used for initial layout and subsequent allocations. */
    private final long initialSnodStorageSize = SNOD_V1_HEADER_SIZE + (DEFAULT_INITIAL_SNOD_ENTRY_COUNT * SNOD_V1_ENTRY_SIZE);
    private long initialObjectHeaderStorageSize = 256L; // Variable storage for messages in the initial OH
    private long initialMessageContinuationSize = 0L; // Variable size for continuation block

    // --- Tracking for Active/Expandable Structures (Sizes now long) ---
    /** Tracks the offset of the *currently active* Local Heap contents block. Starts same as root, changes on expansion. */
    private long currentLocalHeapContentsOffset;
    /** Tracks the size (long) of the *currently active* Local Heap contents block. Starts same as root, changes on expansion. */
    private long currentLocalHeapContentsSize;

    // --- Allocation Tracking ---
    /** Tracks the offset immediately after the last allocated structure. */
    private long nextAvailableOffset;

    // --- Other Calculated Offsets ---
    private long globalHeapOffset = -1L; // Indicate not yet calculated


    /**
     * Constructor: Initializes the allocation manager and calculates the
     * layout of the initial, fixed metadata block.
     */
    public HdfFileAllocation() {
        recalcInitialLayout();
    }

    /**
     * Recalculates the offsets of the structures within the *initial* metadata block.
     * This uses the fixed initial sizes (like rootLocalHeapContentsSize).
     * It should be called if sizes *within this initial block* change (e.g.,
     * initialObjectHeaderStorageSize). It also ensures `nextAvailableOffset`
     * points correctly past the end of this block, unless dynamic allocations
     * have already occurred further out.
     */
    private void recalcInitialLayout() {
        long currentOffset = SUPERBLOCK_OFFSET;

        currentOffset += SUPERBLOCK_SIZE;
        rootObjectHeaderPrefixOffset = currentOffset;

        currentOffset += ROOT_OBJECT_HEADER_PREFIX_SIZE;
        rootBtreeOffset = currentOffset;

        currentOffset += BTREE_NODE_SIZE + rootBtreeStorageSize;
        rootLocalHeapOffset = currentOffset; // Offset of the LHeap Header

        currentOffset += LOCAL_HEAP_HEADER_SIZE;
        rootLocalHeapContentsOffset = currentOffset;

        currentOffset += rootLocalHeapContentsSize;
        initialSnodOffset = currentOffset; // Place first SNOD here

        currentOffset += initialSnodStorageSize;
        initialObjectHeaderOffset = currentOffset; // Place first "user" object header here

        currentOffset += OBJECT_HEADER_MINIMUM_SIZE + initialObjectHeaderStorageSize;
        if (initialMessageContinuationSize > 0L) { // Check against long zero
            initialMessageContinuationOffset = currentOffset;
            currentOffset += initialMessageContinuationSize;
        } else {
            initialMessageContinuationOffset = -1L; // Indicate no allocation
        }

        long endOfInitialBlock = currentOffset;

        // Initialize current/active heap tracking based on the initial root values
        currentLocalHeapContentsOffset = rootLocalHeapContentsOffset;
        currentLocalHeapContentsSize = rootLocalHeapContentsSize;

        if (nextAvailableOffset > 0L) { // Check against long zero
            nextAvailableOffset = Math.max(nextAvailableOffset, endOfInitialBlock);
        } else {
            nextAvailableOffset = endOfInitialBlock;
        }

        dataOffset = Math.max(MIN_DATA_OFFSET, endOfInitialBlock);
        globalHeapOffset = -1L; // Reset global heap offset
    }

    // --- Methods to Modify Initial Layout ---

    /**
     * Sets the storage size (long) required for the messages within the initial object header
     * and recalculates the layout.
     *
     * @param storageSize The required size in bytes (must be non-negative).
     * @throws IllegalArgumentException if storageSize is negative.
     */
    public void setInitialObjectHeaderStorageSize(long storageSize) { // Parameter is long
        if (storageSize < 0L) { // Check against long zero
            throw new IllegalArgumentException("Object Header storage size cannot be negative.");
        }
        this.initialObjectHeaderStorageSize = storageSize;
        recalcInitialLayout();
    }

    /**
     * Sets the size (long) required for the message continuation block associated with the
     * initial object header and recalculates the layout.
     *
     * @param continuationSize The required size in bytes (must be non-negative).
     * @throws IllegalArgumentException if continuationSize is negative.
     */
    public void setInitialMessageContinuationSize(long continuationSize) { // Parameter is long
        if (continuationSize < 0L) { // Check against long zero
            throw new IllegalArgumentException("Message continuation size cannot be negative.");
        }
        this.initialMessageContinuationSize = continuationSize;
        recalcInitialLayout();
    }

    /**
     * Sets the storage size for the initial object header and the size for its
     * associated message continuation block, then recalculates the layout once.
     *
     * @param storageSize The required storage size in bytes (must be non-negative).
     * @param continuationSize The required continuation size in bytes (must be non-negative).
     * @throws IllegalArgumentException if either size is negative.
     */
    public void setInitialObjectHeaderAndContinuationSizes(long storageSize, long continuationSize) { // Parameters are long
        if (storageSize < 0L || continuationSize < 0L) {
            throw new IllegalArgumentException("Sizes cannot be negative.");
        }
        this.initialObjectHeaderStorageSize = storageSize;
        this.initialMessageContinuationSize = continuationSize;
        recalcInitialLayout(); // Recalculate only once
    }


    // --- Methods for Dynamic Allocation (Append Strategy) ---

    /**
     * Allocates space for a new Symbol Table Node (SNOD) block (header + entries)
     * at the next available offset. The size allocated is the same as the initial SNOD block.
     *
     * @return The starting offset of the allocated SNOD block.
     * @throws IllegalStateException if the initial SNOD storage size is not positive.
     */
    public long allocateNextSnodStorage() {
        long allocationSize = this.initialSnodStorageSize; // Already long
        if (allocationSize <= 0L) { // Check against long zero
            throw new IllegalStateException("Initial SNOD storage size is not valid (must be positive) for allocation.");
        }
        return allocateGenericBlock(allocationSize);
    }

    /**
     * Allocates space for a new Object Header block (minimum header + message storage)
     * at the next available offset.
     *
     * @param messageStorageSize The size (long) needed for storing header messages.
     * @return The starting offset of the allocated Object Header block.
     * @throws IllegalArgumentException if messageStorageSize is negative.
     */
    public long allocateNextObjectHeader(long messageStorageSize) { // Parameter is long
        if (messageStorageSize < 0L) { // Check against long zero
            throw new IllegalArgumentException("Object Header message storage size cannot be negative.");
        }
        long allocationOffset = nextAvailableOffset;
        // Size calculation uses long constants and parameters
        long allocationSize = OBJECT_HEADER_MINIMUM_SIZE + messageStorageSize;
        nextAvailableOffset += allocationSize;
        return allocationOffset;
    }

    /**
     * Allocates space for a new Message Continuation block at the next available offset.
     *
     * @param continuationSize The required size (long) of the continuation block.
     * @return The starting offset of the allocated Message Continuation block.
     * @throws IllegalArgumentException if continuationSize is not positive.
     */
    public long allocateNextMessageContinuation(long continuationSize) { // Parameter is long
        if (continuationSize <= 0L) { // Check against long zero
            throw new IllegalArgumentException("Message continuation size must be positive.");
        }
        return allocateGenericBlock(continuationSize);
    }

    /**
     * Allocates a generic block of space of a given size at the next available offset.
     *
     * @param size The number of bytes (long) to allocate. Must be positive.
     * @return The starting offset of the allocated block.
     * @throws IllegalArgumentException if size is not positive.
     */
    public long allocateGenericBlock(long size) { // Parameter is long
        if (size <= 0L) { // Check against long zero
            throw new IllegalArgumentException("Allocation size must be positive.");
        }
        long allocationOffset = nextAvailableOffset;
        nextAvailableOffset += size;
        return allocationOffset;
    }

    /**
     * Expands the storage for the Local Heap contents.
     * Allocates a new block, double the size of the current active block,
     * at the next available offset. Updates the tracking fields for the
     * current active heap location and size.
     *
     * @return The starting offset of the newly allocated (expanded) heap contents block.
     * @throws IllegalStateException if the current heap size is zero or negative.
     */
    public long expandLocalHeapContents() {
        long oldSize = this.currentLocalHeapContentsSize; // Already long
        if (oldSize <= 0L) { // Check against long zero
            throw new IllegalStateException("Cannot expand heap with zero or negative current size: " + oldSize);
        }
        long newSize = oldSize * 2L; // Ensure long multiplication
        long newOffset = allocateGenericBlock(newSize); // Allocate space at the end

        // Update the tracking pointers to the *new* heap block
        this.currentLocalHeapContentsOffset = newOffset;
        this.currentLocalHeapContentsSize = newSize;

        // System.out.println("Expanded Local Heap: Old Size=" + oldSize + ", New Size=" + newSize + ", New Offset=" + newOffset); // Debugging

        return newOffset; // Return the offset of the new block
    }


    // --- Global Heap ---

    /**
     * Computes and stores the starting offset of the Global Heap.
     * Assumes Global Heap follows the data segment.
     *
     * @param totalDataSegmentSize The total size (long) of all dataset raw data in bytes.
     * @throws IllegalArgumentException if totalDataSegmentSize is negative.
     * @throws IllegalStateException if the data offset hasn't been calculated correctly.
     */
    public void computeGlobalHeapOffset(long totalDataSegmentSize) { // Parameter is long
        if (totalDataSegmentSize < 0L) { // Check against long zero
            throw new IllegalArgumentException("Total data segment size cannot be negative.");
        }
        if (dataOffset < 0L ) { // Check against long zero
            throw new IllegalStateException("Data offset has not been calculated correctly (is " + dataOffset + "). Cannot compute Global Heap offset.");
        }
        // Global Heap starts immediately after the data blocks
        this.globalHeapOffset = dataOffset + totalDataSegmentSize;

        // Update nextAvailableOffset if the Global Heap location extends beyond current allocations
        this.nextAvailableOffset = Math.max(this.nextAvailableOffset, this.globalHeapOffset);
    }

    // --- Getters ---
    // Lombok @Getter provides public getters for all fields (including the now-long sizes).

    /**
     * Gets the total size allocated or reserved by the metadata structures
     * managed by this allocator so far. This represents the current effective
     * end-of-file marker from the perspective of allocation.
     *
     * @return The next available offset (long), which is the total size used/reserved.
     */
    public long getTotalAllocatedSize() {
        return nextAvailableOffset; // Already long
    }
}