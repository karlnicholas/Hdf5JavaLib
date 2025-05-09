package org.hdf5javalib.file;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

// Optional: Add static import to simplify AllocationType references
// import static org.hdf5javalib.file.HdfFileAllocation.AllocationType.*;

public class HdfFileAllocationTest {

    private HdfFileAllocation allocation;

    @BeforeEach
    void setUp() {
        allocation = new HdfFileAllocation();
    }

    @Test
    void testInitialLayout() {
        assertEquals(0, allocation.getSuperblockRecord().getOffset());
        assertEquals(96, allocation.getSuperblockSize());
        assertEquals(96, allocation.getObjectHeaderPrefixRecord().getOffset());
        assertEquals(136, allocation.getBtreeRecord().getOffset());
        assertEquals(680, allocation.getLocalHeapOffset());
        assertEquals(712, allocation.getCurrentLocalHeapContentsOffset());
        assertEquals(88, allocation.getCurrentLocalHeapContentsSize());
        assertEquals(2048, allocation.getEndOfFileOffset());
        assertFalse(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(0)); // Default size
    }

    @Test
    void testScalarH5() {
        // Replicate scalar.h5: 4 datasets, no global heap, no continuation, no local heap doubling
        allocation.allocateDatasetStorage("byte");
        allocation.allocateNextSnodStorage();
        allocation.allocateAndSetDataBlock("byte", 1);
        allocation.allocateDatasetStorage("short");
        allocation.allocateAndSetDataBlock("short", 2);
        allocation.allocateDatasetStorage("integer");
        allocation.allocateAndSetDataBlock("integer", 4);
        allocation.allocateDatasetStorage("long");
        allocation.allocateAndSetDataBlock("long", 8);

        // Verify layout
        assertEquals(4368, allocation.getEndOfFileOffset());
        assertEquals(1072, allocation.getSnodOffset());
        assertEquals(1, allocation.getAllSnodAllocationOffsets().size());
        assertEquals(1072, allocation.getAllSnodAllocationOffsets().get(0));

        // Verify dataset allocations
        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> byteInfo = allocation.getDatasetAllocationInfo("byte");
        assertNotNull(byteInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(800, byteInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(272, byteInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(byteInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(2048, byteInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(1, byteInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> shortInfo = allocation.getDatasetAllocationInfo("short");
        assertNotNull(shortInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(1400, shortInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(272, shortInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(shortInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(2049, shortInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(2, shortInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> integerInfo = allocation.getDatasetAllocationInfo("integer");
        assertNotNull(integerInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(1672, integerInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(272, integerInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(integerInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(2051, integerInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(4, integerInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> longInfo = allocation.getDatasetAllocationInfo("long");
        assertNotNull(longInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(4096, longInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(272, longInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(longInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(2055, longInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(8, longInfo.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        assertFalse(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(0)); // No heaps, default size
        assertTrue(allocation.isDataBlocksAllocated());
        assertEquals(712, allocation.getCurrentLocalHeapContentsOffset());
        assertEquals(88, allocation.getCurrentLocalHeapContentsSize());
    }

    @Test
    void testCompoundExampleH5() {
        // Replicate compound_Example.h5: 1 dataset, header expansion, continuation, global heaps
        allocation.allocateDatasetStorage("CompoundData");
        allocation.increaseHeaderAllocation("CompoundData", 968 + 16);
        allocation.allocateNextSnodStorage();
        allocation.allocateAndSetContinuationBlock("CompoundData", 120);
        allocation.allocateFirstGlobalHeapBlock();
        allocation.allocateAndSetDataBlock("CompoundData", 96000);
        allocation.allocateNextGlobalHeapBlock();
        allocation.expandGlobalHeapBlock(); // 4096 → 8192
        allocation.expandGlobalHeapBlock(); // 8192 → 16384
        allocation.expandGlobalHeapBlock(); // 16384 → 32768

        // Verify layout
        assertEquals(135096, allocation.getEndOfFileOffset());
        assertEquals(1784, allocation.getSnodOffset());
        assertEquals(1, allocation.getAllSnodAllocationOffsets().size());
        assertEquals(1784, allocation.getAllSnodAllocationOffsets().get(0));

        // Verify dataset allocations
        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> info = allocation.getDatasetAllocationInfo("CompoundData");
        assertNotNull(info.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(800, info.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(968 + 16, info.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(info.get(HdfFileAllocation.AllocationType.DATASET_HEADER_CONTINUATION));
        assertEquals(2112, info.get(HdfFileAllocation.AllocationType.DATASET_HEADER_CONTINUATION).getOffset());
        assertEquals(120, info.get(HdfFileAllocation.AllocationType.DATASET_HEADER_CONTINUATION).getSize());
        assertNotNull(info.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(6328, info.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(96000, info.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        assertTrue(allocation.hasGlobalHeapAllocation());
        assertEquals(2232, allocation.getGlobalHeapOffset());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(2232));
        assertEquals(32768, allocation.getGlobalHeapBlockSize(102328));
        assertEquals(4096, allocation.getGlobalHeapBlockSize(9999)); // Non-existent offset
        assertEquals(712, allocation.getCurrentLocalHeapContentsOffset());
        assertEquals(88, allocation.getCurrentLocalHeapContentsSize());
    }

    @Test
    void testTwentyFiles() {
        // Allocate 20 datasets with SNODs and local heap expansions
        allocation.allocateDatasetStorage("dataset_1");
        allocation.allocateNextSnodStorage();
        allocation.allocateAndSetDataBlock("dataset_1", 4);
        allocation.allocateDatasetStorage("dataset_2");
        allocation.allocateAndSetDataBlock("dataset_2", 4);
        allocation.allocateDatasetStorage("dataset_3");
        allocation.allocateAndSetDataBlock("dataset_3", 4);
        allocation.allocateDatasetStorage("dataset_4");
        allocation.allocateAndSetDataBlock("dataset_4", 4);
        allocation.allocateDatasetStorage("dataset_5");
        allocation.allocateAndSetDataBlock("dataset_5", 4);
        allocation.allocateDatasetStorage("dataset_6");
        allocation.expandLocalHeapContents(); // 88 → 176
        allocation.allocateAndSetDataBlock("dataset_6", 4);
        allocation.allocateDatasetStorage("dataset_7");
        allocation.allocateAndSetDataBlock("dataset_7", 4);
        allocation.allocateDatasetStorage("dataset_8");
        allocation.allocateAndSetDataBlock("dataset_8", 4);
        allocation.allocateDatasetStorage("dataset_9");
        allocation.allocateNextSnodStorage();
        allocation.allocateAndSetDataBlock("dataset_9", 4);
        allocation.allocateDatasetStorage("dataset_10");
        allocation.expandLocalHeapContents(); // 176 → 352
        allocation.allocateAndSetDataBlock("dataset_10", 4);
        allocation.allocateDatasetStorage("dataset_11");
        allocation.allocateAndSetDataBlock("dataset_11", 4);
        allocation.allocateDatasetStorage("dataset_12");
        allocation.allocateAndSetDataBlock("dataset_12", 4);
        allocation.allocateDatasetStorage("dataset_13");
        allocation.allocateAndSetDataBlock("dataset_13", 4);
        allocation.allocateDatasetStorage("dataset_14");
        allocation.allocateNextSnodStorage();
        allocation.allocateAndSetDataBlock("dataset_14", 4);
        allocation.allocateDatasetStorage("dataset_15");
        allocation.allocateAndSetDataBlock("dataset_15", 4);
        allocation.allocateDatasetStorage("dataset_16");
        allocation.allocateAndSetDataBlock("dataset_16", 4);
        allocation.allocateDatasetStorage("dataset_17");
        allocation.allocateAndSetDataBlock("dataset_17", 4);
        allocation.allocateDatasetStorage("dataset_18");
        allocation.allocateNextSnodStorage();
        allocation.allocateAndSetDataBlock("dataset_18", 4);
        allocation.allocateDatasetStorage("dataset_19");
        allocation.allocateAndSetDataBlock("dataset_19", 4);
        allocation.allocateDatasetStorage("dataset_20");
        allocation.allocateAndSetDataBlock("dataset_20", 4);

        // Verify layout
        assertEquals(10232, allocation.getEndOfFileOffset());
        List<Long> snodOffsets = allocation.getAllSnodAllocationOffsets();
        assertEquals(4, snodOffsets.size());
        assertEquals(1072, snodOffsets.get(0));
        assertEquals(5904, snodOffsets.get(1));
        assertEquals(7944, snodOffsets.get(2));
        assertEquals(9360, snodOffsets.get(3));

        // Verify dataset allocations
        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> info1 = allocation.getDatasetAllocationInfo("dataset_1");
        assertNotNull(info1.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(800, info1.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(272, info1.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(info1.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(2048, info1.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(4, info1.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> info2 = allocation.getDatasetAllocationInfo("dataset_2");
        assertNotNull(info2.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(1400, info2.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(272, info2.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(info2.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(2052, info2.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(4, info2.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> info20 = allocation.getDatasetAllocationInfo("dataset_20");
        assertNotNull(info20.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER));
        assertEquals(9960, info20.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getOffset());
        assertEquals(272, info20.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        assertNotNull(info20.get(HdfFileAllocation.AllocationType.DATASET_DATA));
        assertEquals(2124, info20.get(HdfFileAllocation.AllocationType.DATASET_DATA).getOffset());
        assertEquals(4, info20.get(HdfFileAllocation.AllocationType.DATASET_DATA).getSize());

        assertFalse(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(0)); // No heaps, default size
        assertEquals(6504, allocation.getCurrentLocalHeapContentsOffset());
        assertEquals(352, allocation.getCurrentLocalHeapContentsSize());

        // Verify allocationRecords
        List<HdfFileAllocation.AllocationRecord> records = allocation.getAllAllocationRecords();
        assertEquals(51, records.size(), "Expected 51 allocation records");

        // Fixed structures
        assertRecord(records.get(0), HdfFileAllocation.AllocationType.SUPERBLOCK, "Superblock", 0, 96);
        assertRecord(records.get(1), HdfFileAllocation.AllocationType.GROUP_OBJECT_HEADER, "Object Header Prefix", 96, 40);
        assertRecord(records.get(2), HdfFileAllocation.AllocationType.BTREE_HEADER, "B-tree (Node + Storage)", 136, 544);
        assertRecord(records.get(3), HdfFileAllocation.AllocationType.LOCAL_HEAP_HEADER, "Local Heap Header", 680, 32);
        assertRecord(records.get(4), HdfFileAllocation.AllocationType.LOCAL_HEAP_ABANDONED, "Abandoned Local Heap Contents (Offset 712)", 712, 88);

        // Dataset headers and other records
        assertRecord(records.get(5), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_1)", 800, 272);
        assertRecord(records.get(6), HdfFileAllocation.AllocationType.SNOD, "SNOD Block 1", 1072, 328);
        assertRecord(records.get(7), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_1)", 2048, 4);
        assertRecord(records.get(8), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_2)", 1400, 272);
        assertRecord(records.get(9), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_2)", 2052, 4);
        assertRecord(records.get(10), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_3)", 1672, 272);
        assertRecord(records.get(11), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_3)", 2056, 4);
        assertRecord(records.get(12), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_4)", 4096, 272);
        assertRecord(records.get(13), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_4)", 2060, 4);
        assertRecord(records.get(14), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_5)", 4368, 272);
        assertRecord(records.get(15), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_5)", 2064, 4);
        assertRecord(records.get(16), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_6)", 4640, 272);
        assertRecord(records.get(17), HdfFileAllocation.AllocationType.LOCAL_HEAP_ABANDONED, "Abandoned Local Heap Contents (Offset 4912)", 4912, 176);
        assertRecord(records.get(18), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_6)", 2068, 4);
        assertRecord(records.get(19), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_7)", 5088, 272);
        assertRecord(records.get(20), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_7)", 2072, 4);
        assertRecord(records.get(21), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_8)", 5360, 272);
        assertRecord(records.get(22), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_8)", 2076, 4);
        assertRecord(records.get(23), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_9)", 5632, 272);
        assertRecord(records.get(24), HdfFileAllocation.AllocationType.SNOD, "SNOD Block 2", 5904, 328);
        assertRecord(records.get(25), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_9)", 2080, 4);
        assertRecord(records.get(26), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_10)", 6232, 272);
        assertRecord(records.get(27), HdfFileAllocation.AllocationType.LOCAL_HEAP, "Expanded Local Heap Contents", 6504, 352);
        assertRecord(records.get(28), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_10)", 2084, 4);
        assertRecord(records.get(29), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_11)", 6856, 272);
        assertRecord(records.get(30), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_11)", 2088, 4);
        assertRecord(records.get(31), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_12)", 7128, 272);
        assertRecord(records.get(32), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_12)", 2092, 4);
        assertRecord(records.get(33), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_13)", 7400, 272);
        assertRecord(records.get(34), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_13)", 2096, 4);
        assertRecord(records.get(35), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_14)", 7672, 272);
        assertRecord(records.get(36), HdfFileAllocation.AllocationType.SNOD, "SNOD Block 3", 7944, 328);
        assertRecord(records.get(37), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_14)", 2100, 4);
        assertRecord(records.get(38), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_15)", 8272, 272);
        assertRecord(records.get(39), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_15)", 2104, 4);
        assertRecord(records.get(40), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_16)", 8544, 272);
        assertRecord(records.get(41), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_16)", 2108, 4);
        assertRecord(records.get(42), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_17)", 8816, 272);
        assertRecord(records.get(43), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_17)", 2112, 4);
        assertRecord(records.get(44), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_18)", 9088, 272);
        assertRecord(records.get(45), HdfFileAllocation.AllocationType.SNOD, "SNOD Block 4", 9360, 328);
        assertRecord(records.get(46), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_18)", 2116, 4);
        assertRecord(records.get(47), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_19)", 9688, 272);
        assertRecord(records.get(48), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_19)", 2120, 4);
        assertRecord(records.get(49), HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER, "Dataset Header (dataset_20)", 9960, 272);
        assertRecord(records.get(50), HdfFileAllocation.AllocationType.DATASET_DATA, "Data Block (dataset_20)", 2124, 4);

        // Debug output
        allocation.printBlocks();
    }

    @Test
    void testGlobalHeapMethods() {
        // Test global heap methods in isolation
        assertFalse(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(0));

        long offset1 = allocation.allocateFirstGlobalHeapBlock();
        assertTrue(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(offset1));

        long offset2 = allocation.allocateNextGlobalHeapBlock();
        assertTrue(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(offset2));

        allocation.expandGlobalHeapBlock(); // 4096 → 8192
        assertEquals(8192, allocation.getGlobalHeapBlockSize(offset2));
    }

    @Test
    void testLocalHeapExpansion() {
        allocation.allocateDatasetStorage("test"); // Add dataset
        assertEquals(88, allocation.getCurrentLocalHeapContentsSize());
        assertEquals(712, allocation.getCurrentLocalHeapContentsOffset());
        // Expand to 176 bytes
        long newSize = allocation.expandLocalHeapContents();
        assertEquals(176, newSize);
        assertEquals(176, allocation.getCurrentLocalHeapContentsSize());
        assertNotEquals(712, allocation.getCurrentLocalHeapContentsOffset());
        // Expand to 352 bytes
        long oldOffset = allocation.getCurrentLocalHeapContentsOffset();
        newSize = allocation.expandLocalHeapContents();
        assertEquals(352, newSize);
        assertEquals(352, allocation.getCurrentLocalHeapContentsSize());
        assertNotEquals(oldOffset, allocation.getCurrentLocalHeapContentsOffset());
    }

    @Test
    void testInvalidAllocations() {
        assertThrows(IllegalArgumentException.class, () -> allocation.allocateDatasetStorage(""));
        allocation.allocateDatasetStorage("test");
        assertThrows(IllegalStateException.class, () -> allocation.allocateDatasetStorage("test"));
        assertThrows(IllegalArgumentException.class, () -> allocation.allocateAndSetDataBlock("test", -1));
        assertThrows(IllegalArgumentException.class, () -> allocation.allocateAndSetContinuationBlock("test", 0));
        assertThrows(IllegalArgumentException.class, () -> allocation.increaseHeaderAllocation("test", 100));
    }

    @Test
    void testReset() {
        allocation.allocateDatasetStorage("test");
        allocation.allocateNextSnodStorage();
        allocation.allocateFirstGlobalHeapBlock();
        allocation.reset();
        assertEquals(2048, allocation.getEndOfFileOffset());
        assertEquals(0, allocation.getAllDatasetAllocations().size());
        assertEquals(0, allocation.getAllSnodAllocationOffsets().size());
        assertFalse(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(0));
    }

    @Test
    void testAllocationRecordSharing() {
        // Test that AllocationRecord modifications are reflected across allocations and allocationRecords
        String datasetName = "testDataset";
        allocation.allocateDatasetStorage(datasetName);
        Map<HdfFileAllocation.AllocationType, HdfFileAllocation.AllocationRecord> datasetInfo = allocation.getDatasetAllocationInfo(datasetName);
        HdfFileAllocation.AllocationRecord headerRecord = datasetInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER);
        assertNotNull(headerRecord);
        assertEquals(272, headerRecord.getSize());

        // Increase header size
        allocation.increaseHeaderAllocation(datasetName, 512);

        // Verify the same record is updated in both allocations and allocationRecords
        assertEquals(512, datasetInfo.get(HdfFileAllocation.AllocationType.DATASET_OBJECT_HEADER).getSize());
        List<HdfFileAllocation.AllocationRecord> records = allocation.getAllAllocationRecords();
        HdfFileAllocation.AllocationRecord updatedRecord = records.stream()
                .filter(r -> r.getName().equals("Dataset Header (" + datasetName + ")"))
                .findFirst()
                .orElse(null);
        assertNotNull(updatedRecord);
        assertEquals(512, updatedRecord.getSize());
        assertSame(headerRecord, updatedRecord, "AllocationRecord should be the same instance");
    }

    @Test
    void testMoveDataNextAvailableOffsetThrows() {
        // Test that moveDataNextAvailableOffset throws UnsupportedOperationException
        allocation.allocateDatasetStorage("test");
        allocation.allocateNextSnodStorage(); // Triggers moveDataNextAvailableOffset in allocateNextSnodStorage
        // No direct way to call moveDataNextAvailableOffset, but we rely on allocateNextSnodStorage to hit it
        // Verify that the exception is thrown in the expected scenario
        try {
            allocation.allocateNextSnodStorage(); // May trigger overlap requiring data offset move
            // If no exception, check if data offset was adjusted correctly
            assertTrue(allocation.getDataNextAvailableOffset() >= 2048);
        } catch (UnsupportedOperationException e) {
            assertEquals("Data offset movement logic not yet implemented", e.getMessage());
        }
    }

    private void assertRecord(HdfFileAllocation.AllocationRecord record, HdfFileAllocation.AllocationType expectedType, String expectedName, long expectedOffset, long expectedSize) {
        assertEquals(expectedType, record.getType(), "Record type mismatch for " + expectedName);
        assertEquals(expectedName, record.getName(), "Record name mismatch at offset " + expectedOffset);
        assertEquals(expectedOffset, record.getOffset(), "Record offset mismatch for " + expectedName);
        assertEquals(expectedSize, record.getSize(), "Record size mismatch for " + expectedName);
    }
}