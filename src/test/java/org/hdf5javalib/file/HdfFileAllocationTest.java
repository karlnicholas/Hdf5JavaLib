package org.hdf5javalib.file;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class HdfFileAllocationTest {

    private HdfFileAllocation allocation;

    @BeforeEach
    void setUp() {
        allocation = new HdfFileAllocation();
    }

    @Test
    void testInitialLayout() {
        assertEquals(0, allocation.getSuperblockOffset());
        assertEquals(96, allocation.getSuperblockSize());
        assertEquals(96, allocation.getObjectHeaderPrefixOffset());
        assertEquals(136, allocation.getBtreeOffset());
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

        HdfFileAllocation.DatasetAllocationInfo byteInfo = allocation.getDatasetAllocationInfo("byte");
        assertEquals(800, byteInfo.getHeaderOffset());
        assertEquals(272, byteInfo.getHeaderSize());
        assertEquals(2048, byteInfo.getDataOffset());
        assertEquals(1, byteInfo.getDataSize());

        HdfFileAllocation.DatasetAllocationInfo shortInfo = allocation.getDatasetAllocationInfo("short");
        assertEquals(1400, shortInfo.getHeaderOffset());
        assertEquals(272, shortInfo.getHeaderSize());
        assertEquals(2049, shortInfo.getDataOffset());
        assertEquals(2, shortInfo.getDataSize());

        HdfFileAllocation.DatasetAllocationInfo integerInfo = allocation.getDatasetAllocationInfo("integer");
        assertEquals(1672, integerInfo.getHeaderOffset());
        assertEquals(272, integerInfo.getHeaderSize());
        assertEquals(2051, integerInfo.getDataOffset());
        assertEquals(4, integerInfo.getDataSize());

        HdfFileAllocation.DatasetAllocationInfo longInfo = allocation.getDatasetAllocationInfo("long");
        assertEquals(4096, longInfo.getHeaderOffset());
        assertEquals(272, longInfo.getHeaderSize());
        assertEquals(2055, longInfo.getDataOffset());
        assertEquals(8, longInfo.getDataSize());

        assertFalse(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(0)); // No heaps, default size
        assertTrue(allocation.isDataBlocksAllocated());
        assertEquals(712, allocation.getCurrentLocalHeapContentsOffset());
        assertEquals(88, allocation.getCurrentLocalHeapContentsSize());
    }

    @Test
    void testCompoundExampleH5() {
        // Replicate compound_Example.h5: 1 dataset, header expansion, continuation, global heaps
        allocation.printBlocks();
        allocation.allocateDatasetStorage("CompoundData");
        allocation.printBlocks();
        allocation.increaseHeaderAllocation("CompoundData", 968+16);
        allocation.printBlocks();
        allocation.allocateNextSnodStorage();
        allocation.printBlocks();
        allocation.allocateAndSetContinuationBlock("CompoundData", 120);
        allocation.printBlocks();
        allocation.allocateFirstGlobalHeapBlock();
        allocation.printBlocks();
        allocation.allocateAndSetDataBlock("CompoundData", 96000);
        allocation.printBlocks();
        allocation.allocateNextGlobalHeapBlock();
        allocation.printBlocks();
        allocation.expandGlobalHeapBlock(); // 4096 → 8192
        allocation.printBlocks();
        allocation.expandGlobalHeapBlock(); // 8192 → 16384
        allocation.printBlocks();
        allocation.expandGlobalHeapBlock(); // 16384 → 32768
        allocation.printBlocks();

        // Verify layout
        assertEquals(135096, allocation.getEndOfFileOffset());
        assertEquals(1784, allocation.getSnodOffset());
        assertEquals(1, allocation.getAllSnodAllocationOffsets().size());
        assertEquals(1784, allocation.getAllSnodAllocationOffsets().get(0));

        HdfFileAllocation.DatasetAllocationInfo info = allocation.getDatasetAllocationInfo("CompoundData");
        assertEquals(800, info.getHeaderOffset());
        assertEquals(968, info.getHeaderSize());
        assertEquals(2112, info.getContinuationOffset());
        assertEquals(120, info.getContinuationSize());
        assertEquals(6328, info.getDataOffset());
        assertEquals(96000, info.getDataSize());

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
        // Replicate 20 files: 20 datasets, multiple SNODs, local heap doubling
        String[] names = new String[20];
        for (int i = 0; i < 20; i++) {
            names[i] = "dataset_" + (i + 1);
        }

        allocation.allocateDatasetStorage(names[0]);
        allocation.allocateNextSnodStorage();
        allocation.allocateAndSetDataBlock(names[0], 4);
        for (int i = 1; i < 20; i++) {
            allocation.allocateDatasetStorage(names[i]);
            allocation.allocateAndSetDataBlock(names[i], 4);
            if (i == 8 || i == 13 || i == 18) {
                allocation.allocateNextSnodStorage();
            }
        }
        allocation.expandLocalHeapContents(); // 88 → 176
        allocation.expandLocalHeapContents(); // 176 → 352

        // Verify layout
        assertEquals(9688, allocation.getEndOfFileOffset());
        List<Long> snodOffsets = allocation.getAllSnodAllocationOffsets();
        assertEquals(4, snodOffsets.size());
        assertEquals(1072, snodOffsets.get(0));
        assertEquals(5904, snodOffsets.get(1));
        assertEquals(7944, snodOffsets.get(2));
        assertEquals(9360, snodOffsets.get(3));

        HdfFileAllocation.DatasetAllocationInfo info1 = allocation.getDatasetAllocationInfo("dataset_1");
        assertEquals(800, info1.getHeaderOffset());
        assertEquals(272, info1.getHeaderSize());
        assertEquals(2048, info1.getDataOffset());
        assertEquals(4, info1.getDataSize());

        HdfFileAllocation.DatasetAllocationInfo info2 = allocation.getDatasetAllocationInfo("dataset_2");
        assertEquals(1400, info2.getHeaderOffset());
        assertEquals(272, info2.getHeaderSize());
        assertEquals(2052, info2.getDataOffset());
        assertEquals(4, info2.getDataSize());

        HdfFileAllocation.DatasetAllocationInfo info20 = allocation.getDatasetAllocationInfo("dataset_20");
        assertEquals(5456, info20.getHeaderOffset());
        assertEquals(272, info20.getHeaderSize());
        assertEquals(2124, info20.getDataOffset());
        assertEquals(4, info20.getDataSize());

        assertFalse(allocation.hasGlobalHeapAllocation());
        assertEquals(4096, allocation.getGlobalHeapBlockSize(0)); // No heaps, default size
        assertEquals(6504, allocation.getCurrentLocalHeapContentsOffset());
        assertEquals(352, allocation.getCurrentLocalHeapContentsSize());
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
}