package org.hdf5javalib.file;

import lombok.Getter;

import java.util.*;

/**
 * Manages HDF5 file allocation layout, assigning offsets and sizes for fixed and dynamic components.
 * <p>
 * The {@code HdfFileAllocation} class handles the allocation of storage blocks for various HDF5 file
 * components, including superblock, object headers, B-trees, local heaps, global heaps, datasets, and
 * symbol table nodes (SNODs). It separates allocations into metadata (800–2047 or free space) and data
 * (2048+) regions, ensuring no overlaps and maintaining alignment boundaries. Each allocation method
 * checks for overlaps, updates next available offsets, and supports dynamic resizing of heaps.
 * </p>
 */
@Getter
public class HdfFileAllocation {

    /**
     * Defines the types of allocation blocks used in an HDF5 file.
     * <p>
     * Each enum constant represents a specific type of storage block, with associated metadata
     * or data region assignments and typical offset and size characteristics.
     * </p>
     */
    public enum AllocationType {
        /** Superblock, located at offset 0 with a size of 96 bytes. */
        SUPERBLOCK,
        /** Object header prefix for the root group, typically at offset 96 with a size of 40 bytes. */
        GROUP_OBJECT_HEADER,
        /** B-tree node and storage, typically at offset 136 with a size of 544 bytes. */
        BTREE_HEADER,
        /** Local heap header, typically at offset 680 with a size of 32 bytes. */
        LOCAL_HEAP_HEADER,
        /** Dataset header, stored in the metadata region. */
        DATASET_OBJECT_HEADER,
        /** Dataset continuation block, stored in the metadata region. */
        DATASET_HEADER_CONTINUATION,
        /** Dataset data block, stored in the data region. */
        DATASET_DATA,
        /** Active local heap contents, stored in the metadata region. */
        LOCAL_HEAP,
        /** Abandoned local heap contents, stored in the metadata region. */
        LOCAL_HEAP_ABANDONED,
        /** Symbol table node (SNOD), stored in the metadata region. */
        SNOD,
        /** First global heap block, stored in the data region with a fixed size. */
        GLOBAL_HEAP_1,
        /** Second global heap block, stored in the data region and expandable. */
        GLOBAL_HEAP_2
    }

    // --- Constants ---
    private static final long SUPERBLOCK_OFFSET = 0L;
    private static final long SUPERBLOCK_SIZE = 96L;
    private static final long OBJECT_HEADER_PREFIX_SIZE = 40L;
    private static final long BTREE_NODE_SIZE = 32L;
    private static final long BTREE_STORAGE_SIZE = 512L;
    private static final long LOCAL_HEAP_HEADER_SIZE = 32L;
    private static final long INITIAL_LOCAL_HEAP_CONTENTS_SIZE = 88L;
    private static final long DATA_OBJECT_HEADER_MESSAGE_SIZE = 16L;
    private static final long SNOD_V1_HEADER_SIZE = 8L;
    private static final long SNOD_V1_ENTRY_SIZE = 32L;
    private static final long DEFAULT_SNOD_ENTRY_COUNT = 10L;
    private static final long GLOBAL_HEAP_BLOCK_SIZE = 4096L;
    private static final long DEFAULT_DATASET_HEADER_SIZE = DATA_OBJECT_HEADER_MESSAGE_SIZE + 256L;
    private static final long MIN_DATA_OFFSET_THRESHOLD = 2048L;
    private static final long METADATA_REGION_START = 800L;
    private static final long SNOD_STORAGE_SIZE = SNOD_V1_HEADER_SIZE + (DEFAULT_SNOD_ENTRY_COUNT * SNOD_V1_ENTRY_SIZE);
    private static final long ALIGNMENT_BOUNDARY = 2048L;

    // --- Storage ---
    /** Maps dataset names to their allocation records by type. */
    private final Map<String, Map<AllocationType, AllocationRecord>> datasetRecordsByName = new LinkedHashMap<>();
    /** List of symbol table node (SNOD) allocation records. */
    private final List<AllocationRecord> snodRecords = new ArrayList<>();
    /** Maps global heap block types to their allocation records. */
    private final Map<AllocationType, AllocationRecord> globalHeapBlocks = new HashMap<>();
    /** List of all allocation records. */
    private final List<AllocationRecord> allocationRecords = new ArrayList<>();
    /** List of local heap allocation records (active and abandoned). */
    private final List<AllocationRecord> localHeapRecords = new ArrayList<>();

    // --- Fixed Allocations ---
    private final AllocationRecord superblockRecord;
    private final AllocationRecord objectHeaderPrefixRecord;
    private final AllocationRecord btreeRecord;
    private final AllocationRecord localHeapHeaderRecord;

    // --- Dynamic Tracking ---
    /** The next available offset in the metadata region. */
    private long metadataNextAvailableOffset;
    /** The next available offset in the data region. */
    private long dataNextAvailableOffset;

    // --- Constructor ---
    /**
     * Initializes a new HDF5 file allocation manager.
     * <p>
     * Sets up fixed allocations for the superblock, group object header, B-tree, and local heap
     * header, and initializes the local heap contents. Configures initial offsets for metadata
     * and data regions.
     * </p>
     */
    public HdfFileAllocation() {
        this.metadataNextAvailableOffset = METADATA_REGION_START;
        this.dataNextAvailableOffset = MIN_DATA_OFFSET_THRESHOLD;

        // Initialize fixed structures
        superblockRecord = new AllocationRecord(AllocationType.SUPERBLOCK, "Superblock", SUPERBLOCK_OFFSET, SUPERBLOCK_SIZE);
        objectHeaderPrefixRecord = new AllocationRecord(AllocationType.GROUP_OBJECT_HEADER, "Object Header Prefix", SUPERBLOCK_OFFSET + SUPERBLOCK_SIZE, OBJECT_HEADER_PREFIX_SIZE);
        btreeRecord = new AllocationRecord(AllocationType.BTREE_HEADER, "B-tree (Node + Storage)", objectHeaderPrefixRecord.getOffset() + OBJECT_HEADER_PREFIX_SIZE, BTREE_NODE_SIZE + BTREE_STORAGE_SIZE);
        localHeapHeaderRecord = new AllocationRecord(AllocationType.LOCAL_HEAP_HEADER, "Local Heap Header", btreeRecord.getOffset() + (BTREE_NODE_SIZE + BTREE_STORAGE_SIZE), LOCAL_HEAP_HEADER_SIZE);

        // Initialize local heap
        AllocationRecord initialLocalHeapRecord = new AllocationRecord(AllocationType.LOCAL_HEAP, "Initial Local Heap Contents", localHeapHeaderRecord.getOffset() + LOCAL_HEAP_HEADER_SIZE, INITIAL_LOCAL_HEAP_CONTENTS_SIZE);
        localHeapRecords.add(initialLocalHeapRecord);

        // Add fixed structures to allocationRecords
        allocationRecords.add(superblockRecord);
        allocationRecords.add(objectHeaderPrefixRecord);
        allocationRecords.add(btreeRecord);
        allocationRecords.add(localHeapHeaderRecord);
        allocationRecords.add(initialLocalHeapRecord);
    }

    // --- Allocation Methods ---
    /**
     * Allocates storage for a dataset's object header.
     *
     * @param datasetName the name of the dataset
     * @return the offset of the allocated header
     * @throws IllegalArgumentException if the dataset name is null or empty
     * @throws IllegalStateException if the dataset is already allocated or an overlap occurs
     */
    public long allocateDatasetStorage(String datasetName) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (datasetName.isEmpty()) throw new IllegalArgumentException("Dataset name cannot be empty");
        if (datasetRecordsByName.containsKey(datasetName) && datasetRecordsByName.get(datasetName).containsKey(AllocationType.DATASET_OBJECT_HEADER)) {
            throw new IllegalStateException("Dataset '" + datasetName + "' already allocated");
        }

        long headerSize = DEFAULT_DATASET_HEADER_SIZE;
        if (checkForOverlap(metadataNextAvailableOffset, headerSize)) {
            moveMetadataNextAvailableOffset(metadataNextAvailableOffset, headerSize);
        }

        long headerOffset = metadataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (" + datasetName + ")", headerOffset, headerSize);
        datasetRecordsByName.computeIfAbsent(datasetName, k -> new HashMap<>()).put(AllocationType.DATASET_OBJECT_HEADER, record);
        allocationRecords.add(record);
        metadataNextAvailableOffset += headerSize;
        updateMetadataOffset(metadataNextAvailableOffset);
        return headerOffset;
    }

    /**
     * Increases the size of a dataset's object header allocation.
     *
     * @param datasetName      the name of the dataset
     * @param newTotalHeaderSize the new size of the header
     * @throws IllegalArgumentException if the new size is not larger than the current size
     * @throws IllegalStateException if the dataset is not found
     */
    public void increaseHeaderAllocation(String datasetName, long newTotalHeaderSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        Map<AllocationType, AllocationRecord> datasetAllocs = datasetRecordsByName.get(datasetName);
        if (datasetAllocs == null || !datasetAllocs.containsKey(AllocationType.DATASET_OBJECT_HEADER)) {
            throw new IllegalStateException("Dataset '" + datasetName + "' not found");
        }

        AllocationRecord record = datasetAllocs.get(AllocationType.DATASET_OBJECT_HEADER);
        long oldSize = record.getSize();
        if (newTotalHeaderSize <= oldSize) {
            throw new IllegalArgumentException("New size must be greater than current size");
        }

        // Update size in shared AllocationRecord
        record.setSize(newTotalHeaderSize);

        // Check and move SNOD if overlapped
        moveSnodIfOverlapped(record.getOffset(), newTotalHeaderSize);

        // Update metadataNextAvailableOffset
        metadataNextAvailableOffset = Math.max(metadataNextAvailableOffset, record.getOffset() + newTotalHeaderSize);
        updateMetadataOffset(metadataNextAvailableOffset);
    }

    /**
     * Allocates storage for the next symbol table node (SNOD).
     *
     * @return the offset of the allocated SNOD
     */
    public long allocateNextSnodStorage() {
        if (checkForOverlap(metadataNextAvailableOffset, SNOD_STORAGE_SIZE)) {
            if (snodRecords.isEmpty()) {
                // special case: first SNOD is allocated at the end of the metadata region
                // but overlaps with the dataNextAvailableOffset.
                long metadataNewOffset = metadataNextAvailableOffset + SNOD_STORAGE_SIZE;
                moveDataNextAvailableOffset(metadataNewOffset);
            } else {
                moveMetadataNextAvailableOffset(metadataNextAvailableOffset, SNOD_STORAGE_SIZE);
            }
        }

        long offset = metadataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.SNOD, "SNOD Block " + (snodRecords.size() + 1), offset, SNOD_STORAGE_SIZE);
        snodRecords.add(record);
        allocationRecords.add(record);
        metadataNextAvailableOffset += SNOD_STORAGE_SIZE;
        updateMetadataOffset(metadataNextAvailableOffset);
        return offset;
    }

    /**
     * Allocates a data block for a dataset.
     *
     * @param datasetName the name of the dataset
     * @param dataSize    the size of the data block
     * @return the offset of the allocated data block
     * @throws IllegalArgumentException if the data size is negative
     * @throws IllegalStateException if the dataset or data block is already allocated
     */
    public long allocateAndSetDataBlock(String datasetName, long dataSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (dataSize < 0) throw new IllegalArgumentException("Data size cannot be negative");
        Map<AllocationType, AllocationRecord> datasetAllocs = datasetRecordsByName.get(datasetName);
        if (datasetAllocs == null) {
            throw new IllegalStateException("Dataset '" + datasetName + "' not found");
        }
        if (datasetAllocs.containsKey(AllocationType.DATASET_DATA)) {
            throw new IllegalStateException("Data block for '" + datasetName + "' already allocated");
        }

        long dataOffset = dataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.DATASET_DATA, "Data Block (" + datasetName + ")", dataOffset, dataSize);
        datasetRecordsByName.computeIfAbsent(datasetName, k -> new HashMap<>()).put(AllocationType.DATASET_DATA, record);
        allocationRecords.add(record);
        dataNextAvailableOffset += dataSize;
        updateDataOffset(dataNextAvailableOffset);
        return dataOffset;
    }

    /**
     * Allocates a continuation block for a dataset's object header.
     *
     * @param datasetName     the name of the dataset
     * @param continuationSize the size of the continuation block
     * @return the offset of the allocated continuation block
     * @throws IllegalArgumentException if the continuation size is not positive
     * @throws IllegalStateException if the dataset or continuation block is already allocated
     */
    public long allocateAndSetContinuationBlock(String datasetName, long continuationSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (continuationSize <= 0) throw new IllegalArgumentException("Continuation size must be positive");
        Map<AllocationType, AllocationRecord> datasetAllocs = datasetRecordsByName.get(datasetName);
        if (datasetAllocs == null) {
            throw new IllegalStateException("Dataset '" + datasetName + "' not found");
        }
        if (datasetAllocs.containsKey(AllocationType.DATASET_HEADER_CONTINUATION)) {
            throw new IllegalStateException("Continuation block for '" + datasetName + "' already allocated");
        }

        if (checkForOverlap(metadataNextAvailableOffset, continuationSize)) {
            moveDataNextAvailableOffset(metadataNextAvailableOffset + continuationSize);
        }

        long continuationOffset = metadataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.DATASET_HEADER_CONTINUATION, "Continuation (" + datasetName + ")", continuationOffset, continuationSize);
        datasetRecordsByName.computeIfAbsent(datasetName, k -> new HashMap<>()).put(AllocationType.DATASET_HEADER_CONTINUATION, record);
        allocationRecords.add(record);
        metadataNextAvailableOffset += continuationSize;
        updateMetadataOffset(metadataNextAvailableOffset);
        return continuationOffset;
    }

    /**
     * Allocates the first global heap block.
     *
     * @return the offset of the allocated global heap block
     * @throws IllegalStateException if the first global heap block is already allocated
     */
    public long allocateFirstGlobalHeapBlock() {
        if (globalHeapBlocks.containsKey(AllocationType.GLOBAL_HEAP_1)) {
            throw new IllegalStateException("First global heap already allocated");
        }

        long size = GLOBAL_HEAP_BLOCK_SIZE;
        long offset = dataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.GLOBAL_HEAP_1, "Global Heap Block 1", offset, size);
        globalHeapBlocks.put(AllocationType.GLOBAL_HEAP_1, record);
        allocationRecords.add(record);
        dataNextAvailableOffset += size;
        updateDataOffset(dataNextAvailableOffset);
        return offset;
    }

    /**
     * Allocates the second global heap block.
     *
     * @return the offset of the allocated global heap block
     * @throws IllegalStateException if the maximum number of global heap blocks is reached
     */
    public long allocateNextGlobalHeapBlock() {
        if (globalHeapBlocks.size() >= 2) {
            throw new IllegalStateException("Only two global heap blocks allowed");
        }

        long size = GLOBAL_HEAP_BLOCK_SIZE;
        long offset = dataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.GLOBAL_HEAP_2, "Global Heap Block 2", offset, size);
        globalHeapBlocks.put(AllocationType.GLOBAL_HEAP_2, record);
        allocationRecords.add(record);
        dataNextAvailableOffset += size;
        updateDataOffset(dataNextAvailableOffset);
        return offset;
    }

    /**
     * Expands the second global heap block by doubling its size.
     *
     * @return the offset of the expanded global heap block
     * @throws IllegalStateException if the second global heap block is not yet allocated
     */
    public long expandGlobalHeapBlock() {
        AllocationRecord record = globalHeapBlocks.get(AllocationType.GLOBAL_HEAP_2);
        if (record == null) {
            throw new IllegalStateException("Second global heap block not yet allocated");
        }
        long oldSize = record.getSize();
        long newSize = oldSize * 2;

        record.setSize(newSize);
        dataNextAvailableOffset = record.getOffset() + newSize;
        updateDataOffset(dataNextAvailableOffset);
        return record.getOffset();
    }

    /**
     * Expands the active local heap contents by doubling its size.
     *
     * @return the new size of the local heap contents
     * @throws IllegalStateException if the current heap size is non-positive
     */
    public long expandLocalHeapContents() {
        AllocationRecord activeRecord = localHeapRecords.get(localHeapRecords.size() - 1);
        long oldSize = activeRecord.getSize();
        if (oldSize <= 0) throw new IllegalStateException("Cannot expand heap with non-positive current tracked size: " + oldSize);
        long newSize = oldSize * 2;

        if (checkForOverlap(metadataNextAvailableOffset, newSize)) {
            moveMetadataNextAvailableOffset(metadataNextAvailableOffset, newSize);
        }

        long newOffset = metadataNextAvailableOffset;

        // Update existing LOCAL_HEAP record to indicate abandonment
        activeRecord.setType(AllocationType.LOCAL_HEAP_ABANDONED);
        activeRecord.setName("Abandoned Local Heap Contents (Offset " + activeRecord.getOffset() + ")");

        // Add new record
        AllocationRecord newRecord = new AllocationRecord(AllocationType.LOCAL_HEAP, "Expanded Local Heap Contents", newOffset, newSize);
        localHeapRecords.add(newRecord);
        allocationRecords.add(newRecord);

        metadataNextAvailableOffset += newSize;
        updateMetadataOffset(metadataNextAvailableOffset);
        return newSize;
    }

    /**
     * Resets the allocation manager to its initial state.
     * <p>
     * Clears all dynamic allocations and reinitializes fixed structures and the local heap.
     * </p>
     */
    public void reset() {
        datasetRecordsByName.clear();
        snodRecords.clear();
        globalHeapBlocks.clear();
        allocationRecords.clear();
        localHeapRecords.clear();
        metadataNextAvailableOffset = METADATA_REGION_START;
        dataNextAvailableOffset = MIN_DATA_OFFSET_THRESHOLD;

        // Reinitialize fixed structures
        allocationRecords.add(superblockRecord);
        allocationRecords.add(objectHeaderPrefixRecord);
        allocationRecords.add(btreeRecord);
        allocationRecords.add(localHeapHeaderRecord);

        // Reinitialize local heap
        AllocationRecord initialLocalHeapRecord = new AllocationRecord(AllocationType.LOCAL_HEAP, "Initial Local Heap Contents", localHeapHeaderRecord.getOffset() + LOCAL_HEAP_HEADER_SIZE, INITIAL_LOCAL_HEAP_CONTENTS_SIZE);
        localHeapRecords.add(initialLocalHeapRecord);
        allocationRecords.add(initialLocalHeapRecord);
    }

    // --- Global Heap Methods ---
    /**
     * Checks if any global heap blocks are allocated.
     *
     * @return true if at least one global heap block is allocated, false otherwise
     */
    public boolean hasGlobalHeapAllocation() {
        return !globalHeapBlocks.isEmpty();
    }

    /**
     * Retrieves the size of a global heap block at a given offset.
     *
     * @param offset the offset of the global heap block
     * @return the size of the global heap block, or the default size if not found
     */
    public long getGlobalHeapBlockSize(long offset) {
        for (AllocationRecord record : globalHeapBlocks.values()) {
            if (record.getOffset() == offset) {
                return record.getSize();
            }
        }
        return GLOBAL_HEAP_BLOCK_SIZE;
    }

    // --- Offset and Overlap Management ---
    /**
     * Updates the metadata next available offset and adjusts the data offset if necessary.
     *
     * @param newOffset the new metadata offset
     */
    private void updateMetadataOffset(long newOffset) {
        metadataNextAvailableOffset = newOffset;
        if (metadataNextAvailableOffset >= dataNextAvailableOffset &&
                !isDataBlocksAllocated() &&
                globalHeapBlocks.isEmpty()) {
            dataNextAvailableOffset = metadataNextAvailableOffset;
            updateDataOffset(dataNextAvailableOffset);
        }
    }

    /**
     * Updates the data next available offset and adjusts the metadata offset if necessary.
     *
     * @param newOffset the new data offset
     */
    private void updateDataOffset(long newOffset) {
        dataNextAvailableOffset = newOffset;
        if (metadataNextAvailableOffset >= MIN_DATA_OFFSET_THRESHOLD &&
                metadataNextAvailableOffset < dataNextAvailableOffset) {
            metadataNextAvailableOffset = dataNextAvailableOffset;
        }
    }

    /**
     * Checks if a proposed allocation overlaps with existing allocations.
     *
     * @param offset the start offset of the proposed allocation
     * @param size   the size of the proposed allocation
     * @return true if an overlap is detected, false otherwise
     */
    private boolean checkForOverlap(long offset, long size) {
        long end = offset + size - 1;
        for (AllocationRecord record : allocationRecords) {
            long recordEnd = record.getOffset() + record.getSize() - 1;
            if (offset <= recordEnd && end >= record.getOffset()) {
                return true; // Overlap detected
            }
        }
        return false; // No overlap
    }

    /**
     * Moves the metadata next available offset to avoid overlaps.
     *
     * @param currentOffset the current offset
     * @param size         the size of the allocation
     */
    private void moveMetadataNextAvailableOffset(long currentOffset, long size) {
        long newOffset = Math.max(currentOffset, dataNextAvailableOffset);
        newOffset = ((newOffset + ALIGNMENT_BOUNDARY - 1) / ALIGNMENT_BOUNDARY) * ALIGNMENT_BOUNDARY; // Next 2048-byte boundary
        while (checkForOverlap(newOffset, size)) {
            newOffset += ALIGNMENT_BOUNDARY; // Try next boundary
        }
        metadataNextAvailableOffset = newOffset;
    }

    /**
     * Moves the data next available offset to accommodate a metadata allocation.
     *
     * @param newMetadataOffset the new metadata offset
     */
    private void moveDataNextAvailableOffset(long newMetadataOffset) {
        int diff = 0;
        for (AllocationRecord record: allocationRecords) {
            if (record.getType() == AllocationType.DATASET_DATA || record.getType() == AllocationType.GLOBAL_HEAP_1) {
                if (diff == 0) {
                    diff = (int) (newMetadataOffset - record.getOffset());
                }
                record.setOffset(record.getOffset() + diff);
            }
        }
        dataNextAvailableOffset += diff;
    }

    /**
     * Moves a symbol table node (SNOD) if it overlaps with a dataset header.
     *
     * @param headerOffset the offset of the dataset header
     * @param headerSize   the size of the dataset header
     */
    private void moveSnodIfOverlapped(long headerOffset, long headerSize) {
        if (snodRecords.isEmpty()) {
            return;
        }

        // Check the most recent SNOD
        AllocationRecord snodRecord = snodRecords.get(snodRecords.size() - 1);
        long snodOffset = snodRecord.getOffset();
        long snodEnd = snodOffset + SNOD_STORAGE_SIZE - 1;
        long headerEnd = headerOffset + headerSize - 1;

        if (headerOffset <= snodEnd && headerEnd >= snodOffset) {
            // Overlap detected, move SNOD
            long newSnodOffset = headerOffset + headerSize;
            if (checkForOverlap(newSnodOffset, SNOD_STORAGE_SIZE)) {
                moveMetadataNextAvailableOffset(newSnodOffset, SNOD_STORAGE_SIZE);
                newSnodOffset = metadataNextAvailableOffset;
                metadataNextAvailableOffset += SNOD_STORAGE_SIZE;
            }

            // Update SNOD record
            snodRecord.setOffset(newSnodOffset);

            // Update metadataNextAvailableOffset
            metadataNextAvailableOffset = Math.max(metadataNextAvailableOffset, newSnodOffset + SNOD_STORAGE_SIZE);
        }
    }

    // --- Debugging ---
    /**
     * Prints the current allocation layout with gap and overlap analysis.
     */
    public void printBlocks() {
        System.out.println("=== HDF File Allocation Layout ===");
        System.out.println("Metadata End of File Offset: " + metadataNextAvailableOffset);
        System.out.println("Data End of File Offset: " + dataNextAvailableOffset);
        System.out.println("Current End of File Offset: " + getEndOfFileOffset());
        System.out.println("----------------------------------");

        System.out.println("Offset (Dec) | Offset (Hex) | Size     | Type       | Name");
        List<AllocationRecord> sortedRecords = new ArrayList<>(allocationRecords);
        sortedRecords.sort(Comparator.comparingLong(AllocationRecord::getOffset));
        for (AllocationRecord block : sortedRecords) {
            String hexOffset = String.format("0x%08X", block.getOffset());
            System.out.printf("%-12d | %-12s | %-8d | %-10s | %s%n",
                    block.getOffset(), hexOffset, block.getSize(), block.getType().name(), block.getName());
        }

        // Detect gaps
        System.out.println("--- Gap Analysis ---");
        long lastEnd = 0;
        for (AllocationRecord record : sortedRecords) {
            long start = record.getOffset();
            if (start > lastEnd) {
                long gapSize = start - lastEnd;
                System.out.printf("Gap detected: Offset %d to %d, Size %d%n", lastEnd, start, gapSize);
            }
            lastEnd = Math.max(lastEnd, start + record.getSize());
        }

        // Check for overlaps
        System.out.println("--- Overlap Check ---");
        boolean hasOverlap = false;
        for (int i = 0; i < sortedRecords.size(); i++) {
            AllocationRecord record1 = sortedRecords.get(i);
            long start1 = record1.getOffset();
            long end1 = start1 + record1.getSize() - 1;

            for (int j = i + 1; j < sortedRecords.size(); j++) {
                AllocationRecord record2 = sortedRecords.get(j);
                long start2 = record2.getOffset();
                long end2 = start2 + record2.getSize() - 1;

                if (start1 <= end2 && start2 <= end1) {
                    hasOverlap = true;
                    System.out.printf("Overlap detected between:%n");
                    System.out.printf("  %s (Offset: %d, Size: %d, End: %d)%n",
                            record1.getName(), start1, record1.getSize(), end1);
                    System.out.printf("  %s (Offset: %d, Size: %d, End: %d)%n",
                            record2.getName(), start2, record2.getSize(), end2);
                }
            }
        }
        if (!hasOverlap) {
            System.out.println("No overlaps detected.");
        }

        System.out.println("==================================");
    }

    /**
     * Prints the allocation layout sorted by offset with overlap analysis.
     */
    public void printBlocksSorted() {
        System.out.println("=== HDF File Allocation Layout (Sorted by Offset) ===");
        System.out.println("Metadata End of File Offset: " + metadataNextAvailableOffset);
        System.out.println("Data End of File Offset: " + dataNextAvailableOffset);
        System.out.println("Current End of File Offset: " + getEndOfFileOffset());
        System.out.println("----------------------------------");

        System.out.println("Offset (Dec) | Offset (Hex) | Size     | Type       | Name");
        List<AllocationRecord> sortedRecords = new ArrayList<>(allocationRecords);
        sortedRecords.sort(Comparator.comparingLong(AllocationRecord::getOffset));
        for (AllocationRecord block : sortedRecords) {
            String hexOffset = String.format("0x%08X", block.getOffset());
            System.out.printf("%-12d | %-12s | %-8d | %-10s | %s%n",
                    block.getOffset(), hexOffset, block.getSize(), block.getType().name(), block.getName());
        }

        // Check for overlaps
        System.out.println("--- Overlap Check ---");
        boolean hasOverlap = false;
        for (int i = 0; i < sortedRecords.size(); i++) {
            AllocationRecord record1 = sortedRecords.get(i);
            long start1 = record1.getOffset();
            long end1 = start1 + record1.getSize() - 1;

            for (int j = i + 1; j < sortedRecords.size(); j++) {
                AllocationRecord record2 = sortedRecords.get(j);
                long start2 = record2.getOffset();
                long end2 = start2 + record2.getSize() - 1;

                if (start1 <= end2 && start2 <= end1) {
                    hasOverlap = true;
                    System.out.printf("Overlap detected between:%n");
                    System.out.printf("  %s (Offset: %d, Size: %d, End: %d)%n",
                            record1.getName(), start1, record1.getSize(), end1);
                    System.out.printf("  %s (Offset: %d, Size: %d, End: %d)%n",
                            record2.getName(), start2, record2.getSize(), end2);
                }
            }
        }
        if (!hasOverlap) {
            System.out.println("No overlaps detected.");
        }

        System.out.println("==================================");
    }

    // --- Getters ---
    /**
     * Retrieves the end-of-file offset.
     *
     * @return the maximum of metadata and data next available offsets
     */
    public long getEndOfFileOffset() {
        return Math.max(metadataNextAvailableOffset, dataNextAvailableOffset);
    }

    /**
     * Retrieves the size of the superblock.
     *
     * @return the superblock size
     */
    public long getSuperblockSize() {
        return superblockRecord.getSize();
    }

    /**
     * Retrieves the total size of the B-tree allocation.
     *
     * @return the B-tree size
     */
    public long getBtreeTotalSize() {
        return btreeRecord.getSize();
    }

    /**
     * Retrieves the offset of the first global heap block.
     *
     * @return the offset, or -1 if not allocated
     */
    public long getGlobalHeapOffset() {
        AllocationRecord record = globalHeapBlocks.get(AllocationType.GLOBAL_HEAP_1);
        return record != null ? record.getOffset() : -1L;
    }

    /**
     * Retrieves the offset of the current local heap contents.
     *
     * @return the offset of the active local heap
     */
    public long getCurrentLocalHeapContentsOffset() {
        return localHeapRecords.get(localHeapRecords.size() - 1).getOffset();
    }

    /**
     * Retrieves the size of the current local heap contents.
     *
     * @return the size of the active local heap
     */
    public long getCurrentLocalHeapContentsSize() {
        return localHeapRecords.get(localHeapRecords.size() - 1).getSize();
    }

    /**
     * Checks if any dataset data blocks are allocated.
     *
     * @return true if at least one data block is allocated, false otherwise
     */
    public boolean isDataBlocksAllocated() {
        return datasetRecordsByName.values().stream()
                .anyMatch(datasetAllocs -> datasetAllocs.containsKey(AllocationType.DATASET_DATA));
    }

    /**
     * Retrieves the offset of the local heap header.
     *
     * @return the local heap header offset
     */
    public long getLocalHeapOffset() {
        return localHeapHeaderRecord.getOffset();
    }

    /**
     * Retrieves the offset of the first symbol table node (SNOD).
     *
     * @return the SNOD offset, or -1 if none allocated
     */
    public long getSnodOffset() {
        return snodRecords.isEmpty() ? -1L : snodRecords.get(0).getOffset();
    }

    /**
     * Retrieves the total size of the root group allocations.
     *
     * @return the combined size of the root group components
     */
    public long getRootGroupSize() {
        return objectHeaderPrefixRecord.getSize() + btreeRecord.getSize() + localHeapHeaderRecord.getSize() + localHeapRecords.get(0).getSize();
    }

    /**
     * Retrieves the offset of the root group.
     *
     * @return the root group offset
     */
    public long getRootGroupOffset() {
        return objectHeaderPrefixRecord.getOffset();
    }

    /**
     * Retrieves the offsets of all symbol table nodes (SNODs).
     *
     * @return an unmodifiable list of SNOD offsets
     */
    public List<Long> getAllSnodAllocationOffsets() {
        List<Long> offsets = new ArrayList<>();
        for (AllocationRecord record : snodRecords) {
            offsets.add(record.getOffset());
        }
        return Collections.unmodifiableList(offsets);
    }

    /**
     * Retrieves the allocation information for a dataset.
     *
     * @param datasetName the name of the dataset
     * @return an unmodifiable map of allocation types to records
     */
    public Map<AllocationType, AllocationRecord> getDatasetAllocationInfo(String datasetName) {
        return datasetRecordsByName.getOrDefault(datasetName, Collections.emptyMap());
    }

    /**
     * Retrieves all dataset allocations.
     *
     * @return an unmodifiable map of dataset names to their allocation records
     */
    public Map<String, Map<AllocationType, AllocationRecord>> getAllDatasetAllocations() {
        return Collections.unmodifiableMap(datasetRecordsByName);
    }

    /**
     * Retrieves all allocation records.
     *
     * @return an unmodifiable list of all allocation records
     */
    public List<AllocationRecord> getAllAllocationRecords() {
        return Collections.unmodifiableList(allocationRecords);
    }

    // --- Helper Classes ---
    /**
     * Represents a single allocation record with type, name, offset, and size.
     */
    public static class AllocationRecord {
        private AllocationType type;
        private String name;
        private long offset;
        private long size;

        /**
         * Constructs an allocation record.
         *
         * @param type   the allocation type
         * @param name   the name of the allocation
         * @param offset the starting offset
         * @param size   the size of the allocation
         */
        public AllocationRecord(AllocationType type, String name, long offset, long size) {
            this.type = type;
            this.name = name;
            this.offset = offset;
            this.size = size;
        }

        /**
         * Gets the allocation type.
         *
         * @return the allocation type
         */
        public AllocationType getType() { return type; }

        /**
         * Gets the name of the allocation.
         *
         * @return the allocation name
         */
        public String getName() { return name; }

        /**
         * Gets the starting offset of the allocation.
         *
         * @return the offset
         */
        public long getOffset() { return offset; }

        /**
         * Gets the size of the allocation.
         *
         * @return the size
         */
        public long getSize() { return size; }

        /**
         * Sets the allocation type.
         *
         * @param type the allocation type
         */
        public void setType(AllocationType type) { this.type = type; }

        /**
         * Sets the name of the allocation.
         *
         * @param name the allocation name
         */
        public void setName(String name) { this.name = name; }

        /**
         * Sets the starting offset of the allocation.
         *
         * @param offset the offset
         */
        public void setOffset(long offset) { this.offset = offset; }

        /**
         * Sets the size of the allocation.
         *
         * @param size the size
         */
        public void setSize(long size) { this.size = size; }
    }
}