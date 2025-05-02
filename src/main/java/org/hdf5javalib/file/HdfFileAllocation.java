package org.hdf5javalib.file;

import lombok.Getter;

import java.util.*;

/**
 * Manages HDF5 file allocation layout, assigning offsets and sizes for fixed and dynamic components.
 * Supports metadata (800–2047 or free space) and data (2048+) categories with separate next available offsets.
 * Each allocation method is self-contained, checking for overlaps and updating offsets.
 */
@Getter
public class HdfFileAllocation {

    // --- Allocation Types ---
    public enum AllocationType {
        SUPERBLOCK,          // Superblock (offset 0, size 96)
        GROUP_OBJECT_HEADER, // Object header prefix for root group (offset 96, size 40)
        BTREE_HEADER,        // B-tree node + storage (offset 136, size 544)
        LOCAL_HEAP_HEADER,   // Local heap header (offset 680, size 32)
        DATASET_OBJECT_HEADER, // Dataset header (metadata region)
        DATASET_HEADER_CONTINUATION, // Dataset continuation block (metadata region)
        DATASET_DATA,        // Dataset data block (data region)
        LOCAL_HEAP,          // Active local heap contents (metadata region)
        LOCAL_HEAP_ABANDONED, // Abandoned local heap contents (metadata region)
        SNOD,                // Symbol table node (metadata region)
        GLOBAL_HEAP_1,       // First global heap block (data region, fixed size)
        GLOBAL_HEAP_2        // Second global heap block (data region, expandable)
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
    private final Map<String, Map<AllocationType, AllocationRecord>> allocations = new LinkedHashMap<>();
    private final List<AllocationRecord> snodRecords = new ArrayList<>();
    private final Map<AllocationType, AllocationRecord> globalHeapBlocks = new HashMap<>();
    private final List<AllocationRecord> allocationRecords = new ArrayList<>();
    private final List<AllocationRecord> localHeapRecords = new ArrayList<>();

    // --- Fixed Allocations ---
    private final AllocationRecord superblockRecord;
    private final AllocationRecord objectHeaderPrefixRecord;
    private final AllocationRecord btreeRecord;
    private final AllocationRecord localHeapHeaderRecord;

    // --- Fixed Sizes ---
    private final long btreeTotalSize;
    private final long initialLocalHeapContentsSize = INITIAL_LOCAL_HEAP_CONTENTS_SIZE;

    // --- Dynamic Tracking ---
    private long metadataNextAvailableOffset;
    private long dataNextAvailableOffset;

    // --- Constructor ---
    public HdfFileAllocation() {
        this.btreeTotalSize = BTREE_NODE_SIZE + BTREE_STORAGE_SIZE;
        this.metadataNextAvailableOffset = METADATA_REGION_START;
        this.dataNextAvailableOffset = MIN_DATA_OFFSET_THRESHOLD;

        // Initialize fixed structures
        superblockRecord = new AllocationRecord(AllocationType.SUPERBLOCK, "Superblock", SUPERBLOCK_OFFSET, SUPERBLOCK_SIZE);
        objectHeaderPrefixRecord = new AllocationRecord(AllocationType.GROUP_OBJECT_HEADER, "Object Header Prefix", SUPERBLOCK_OFFSET + SUPERBLOCK_SIZE, OBJECT_HEADER_PREFIX_SIZE);
        btreeRecord = new AllocationRecord(AllocationType.BTREE_HEADER, "B-tree (Node + Storage)", objectHeaderPrefixRecord.getOffset() + OBJECT_HEADER_PREFIX_SIZE, btreeTotalSize);
        localHeapHeaderRecord = new AllocationRecord(AllocationType.LOCAL_HEAP_HEADER, "Local Heap Header", btreeRecord.getOffset() + btreeTotalSize, LOCAL_HEAP_HEADER_SIZE);

        // Initialize local heap
        AllocationRecord initialLocalHeapRecord = new AllocationRecord(AllocationType.LOCAL_HEAP, "Initial Local Heap Contents", localHeapHeaderRecord.getOffset() + LOCAL_HEAP_HEADER_SIZE, initialLocalHeapContentsSize);
        localHeapRecords.add(initialLocalHeapRecord);

        // Add fixed structures to allocationRecords
        allocationRecords.add(superblockRecord);
        allocationRecords.add(objectHeaderPrefixRecord);
        allocationRecords.add(btreeRecord);
        allocationRecords.add(localHeapHeaderRecord);
        allocationRecords.add(initialLocalHeapRecord);
    }

    // --- Allocation Methods ---
    public long allocateDatasetStorage(String datasetName) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (datasetName.isEmpty()) throw new IllegalArgumentException("Dataset name cannot be empty");
        if (allocations.containsKey(datasetName) && allocations.get(datasetName).containsKey(AllocationType.DATASET_OBJECT_HEADER)) {
            throw new IllegalStateException("Dataset '" + datasetName + "' already allocated");
        }

        long headerSize = DEFAULT_DATASET_HEADER_SIZE;
        if (checkForOverlap(metadataNextAvailableOffset, headerSize)) {
            moveMetadataNextAvailableOffset(metadataNextAvailableOffset, headerSize);
        }

        long headerOffset = metadataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (" + datasetName + ")", headerOffset, headerSize);
        allocations.computeIfAbsent(datasetName, k -> new HashMap<>()).put(AllocationType.DATASET_OBJECT_HEADER, record);
        allocationRecords.add(record);
        metadataNextAvailableOffset += headerSize;
        updateMetadataOffset(metadataNextAvailableOffset);
        return headerOffset;
    }

    public void increaseHeaderAllocation(String datasetName, long newTotalHeaderSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        Map<AllocationType, AllocationRecord> datasetAllocs = allocations.get(datasetName);
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

    public long allocateNextSnodStorage() {
        if (checkForOverlap(metadataNextAvailableOffset, SNOD_STORAGE_SIZE)) {
            if (snodRecords.isEmpty()) {
                // special case: first SNOD is allocated at the end of the metadata region
                // but overlaps with the dataNextAvailableOffset.
                long moveAmount = ((metadataNextAvailableOffset + SNOD_STORAGE_SIZE) - dataNextAvailableOffset);
                moveDataNextAvailableOffset(dataNextAvailableOffset, moveAmount);
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

    public long allocateAndSetDataBlock(String datasetName, long dataSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (dataSize < 0) throw new IllegalArgumentException("Data size cannot be negative");
        Map<AllocationType, AllocationRecord> datasetAllocs = allocations.get(datasetName);
        if (datasetAllocs == null) {
            throw new IllegalStateException("Dataset '" + datasetName + "' not found");
        }
        if (datasetAllocs.containsKey(AllocationType.DATASET_DATA)) {
            throw new IllegalStateException("Data block for '" + datasetName + "' already allocated");
        }

        if (checkForOverlap(dataNextAvailableOffset, dataSize)) {
            moveDataNextAvailableOffset(dataNextAvailableOffset, dataSize);
        }

        long dataOffset = dataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.DATASET_DATA, "Data Block (" + datasetName + ")", dataOffset, dataSize);
        allocations.computeIfAbsent(datasetName, k -> new HashMap<>()).put(AllocationType.DATASET_DATA, record);
        allocationRecords.add(record);
        dataNextAvailableOffset += dataSize;
        updateDataOffset(dataNextAvailableOffset);
        return dataOffset;
    }

    public long allocateAndSetContinuationBlock(String datasetName, long continuationSize) {
        Objects.requireNonNull(datasetName, "Dataset name cannot be null");
        if (continuationSize <= 0) throw new IllegalArgumentException("Continuation size must be positive");
        Map<AllocationType, AllocationRecord> datasetAllocs = allocations.get(datasetName);
        if (datasetAllocs == null) {
            throw new IllegalStateException("Dataset '" + datasetName + "' not found");
        }
        if (datasetAllocs.containsKey(AllocationType.DATASET_HEADER_CONTINUATION)) {
            throw new IllegalStateException("Continuation block for '" + datasetName + "' already allocated");
        }

        if (checkForOverlap(metadataNextAvailableOffset, continuationSize)) {
            moveMetadataNextAvailableOffset(metadataNextAvailableOffset, continuationSize);
        }

        long continuationOffset = metadataNextAvailableOffset;
        AllocationRecord record = new AllocationRecord(AllocationType.DATASET_HEADER_CONTINUATION, "Continuation (" + datasetName + ")", continuationOffset, continuationSize);
        allocations.computeIfAbsent(datasetName, k -> new HashMap<>()).put(AllocationType.DATASET_HEADER_CONTINUATION, record);
        allocationRecords.add(record);
        metadataNextAvailableOffset += continuationSize;
        updateMetadataOffset(metadataNextAvailableOffset);
        return continuationOffset;
    }

    public long allocateFirstGlobalHeapBlock() {
        if (globalHeapBlocks.containsKey(AllocationType.GLOBAL_HEAP_1)) {
            throw new IllegalStateException("First global heap already allocated");
        }

        if (checkForOverlap(dataNextAvailableOffset, GLOBAL_HEAP_BLOCK_SIZE)) {
            moveDataNextAvailableOffset(dataNextAvailableOffset, GLOBAL_HEAP_BLOCK_SIZE);
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

    public long allocateNextGlobalHeapBlock() {
        if (globalHeapBlocks.size() >= 2) {
            throw new IllegalStateException("Only two global heap blocks allowed");
        }

        if (checkForOverlap(dataNextAvailableOffset, GLOBAL_HEAP_BLOCK_SIZE)) {
            moveDataNextAvailableOffset(dataNextAvailableOffset, GLOBAL_HEAP_BLOCK_SIZE);
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

    public void reset() {
        allocations.clear();
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
        AllocationRecord initialLocalHeapRecord = new AllocationRecord(AllocationType.LOCAL_HEAP, "Initial Local Heap Contents", localHeapHeaderRecord.getOffset() + LOCAL_HEAP_HEADER_SIZE, initialLocalHeapContentsSize);
        localHeapRecords.add(initialLocalHeapRecord);
        allocationRecords.add(initialLocalHeapRecord);
    }

    // --- Global Heap Methods ---
    public boolean hasGlobalHeapAllocation() {
        return !globalHeapBlocks.isEmpty();
    }

    public long getGlobalHeapBlockSize(long offset) {
        for (AllocationRecord record : globalHeapBlocks.values()) {
            if (record.getOffset() == offset) {
                return record.getSize();
            }
        }
        return GLOBAL_HEAP_BLOCK_SIZE;
    }

    // --- Offset and Overlap Management ---
    private void updateMetadataOffset(long newOffset) {
        metadataNextAvailableOffset = newOffset;
        if (metadataNextAvailableOffset >= dataNextAvailableOffset &&
                !isDataBlocksAllocated() &&
                globalHeapBlocks.isEmpty()) {
            dataNextAvailableOffset = metadataNextAvailableOffset;
            updateDataOffset(dataNextAvailableOffset);
        }
    }

    private void updateDataOffset(long newOffset) {
        dataNextAvailableOffset = newOffset;
        if (metadataNextAvailableOffset >= MIN_DATA_OFFSET_THRESHOLD &&
                metadataNextAvailableOffset < dataNextAvailableOffset) {
            metadataNextAvailableOffset = dataNextAvailableOffset;
        }
    }

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

    private void moveMetadataNextAvailableOffset(long currentOffset, long size) {
        long newOffset = Math.max(currentOffset, dataNextAvailableOffset);
        newOffset = ((newOffset + ALIGNMENT_BOUNDARY - 1) / ALIGNMENT_BOUNDARY) * ALIGNMENT_BOUNDARY; // Next 2048-byte boundary
        while (checkForOverlap(newOffset, size)) {
            newOffset += ALIGNMENT_BOUNDARY; // Try next boundary
        }
        metadataNextAvailableOffset = newOffset;
    }

    private void moveDataNextAvailableOffset(long currentOffset, long size) {
        throw new UnsupportedOperationException("Data offset movement logic not yet implemented");
    }

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

            // Update metadataNextAvailableOffset if necessary
            metadataNextAvailableOffset = Math.max(metadataNextAvailableOffset, newSnodOffset + SNOD_STORAGE_SIZE);
        }
    }

    // --- Debugging ---
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
                long end2 = record2.getSize() - 1;

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
    public long getEndOfFileOffset() {
        return Math.max(metadataNextAvailableOffset, dataNextAvailableOffset);
    }

    public long getSuperblockSize() {
        return superblockRecord.getSize();
    }

    public long getGlobalHeapOffset() {
        AllocationRecord record = globalHeapBlocks.get(AllocationType.GLOBAL_HEAP_1);
        return record != null ? record.getOffset() : -1L;
    }

    public long getCurrentLocalHeapContentsOffset() {
        return localHeapRecords.get(localHeapRecords.size() - 1).getOffset();
    }

    public long getCurrentLocalHeapContentsSize() {
        return localHeapRecords.get(localHeapRecords.size() - 1).getSize();
    }

    public boolean isDataBlocksAllocated() {
        return allocations.values().stream()
                .anyMatch(datasetAllocs -> datasetAllocs.containsKey(AllocationType.DATASET_DATA));
    }

    public long getLocalHeapOffset() {
        return localHeapHeaderRecord.getOffset();
    }

    public long getSnodOffset() {
        return snodRecords.isEmpty() ? -1L : snodRecords.get(0).getOffset();
    }

    public long getRootGroupSize() {
        return objectHeaderPrefixRecord.getSize() + btreeRecord.getSize() + localHeapHeaderRecord.getSize() + initialLocalHeapContentsSize;
    }

    public long getRootGroupOffset() {
        return objectHeaderPrefixRecord.getOffset();
    }

    public List<Long> getAllSnodAllocationOffsets() {
        List<Long> offsets = new ArrayList<>();
        for (AllocationRecord record : snodRecords) {
            offsets.add(record.getOffset());
        }
        return Collections.unmodifiableList(offsets);
    }

    public Map<AllocationType, AllocationRecord> getDatasetAllocationInfo(String datasetName) {
        return allocations.getOrDefault(datasetName, Collections.emptyMap());
    }

    public Map<String, Map<AllocationType, AllocationRecord>> getAllDatasetAllocations() {
        return Collections.unmodifiableMap(allocations);
    }

    public List<AllocationRecord> getAllAllocationRecords() { return Collections.unmodifiableList(allocationRecords); }

    // --- Helper Classes ---
    public static class AllocationRecord {
        private AllocationType type; // SUPERBLOCK, DATASET_OBJECT_HEADER, etc.
        private String name;
        private long offset;
        private long size;

        public AllocationRecord(AllocationType type, String name, long offset, long size) {
            this.type = type;
            this.name = name;
            this.offset = offset;
            this.size = size;
        }

        public AllocationType getType() { return type; }
        public String getName() { return name; }
        public long getOffset() { return offset; }
        public long getSize() { return size; }
        public void setType(AllocationType type) { this.type = type; }
        public void setName(String name) { this.name = name; }
        public void setOffset(long offset) { this.offset = offset; }
        public void setSize(long size) { this.size = size; }
    }
}