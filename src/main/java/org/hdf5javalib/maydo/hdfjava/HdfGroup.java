package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.utils.HdfWriteUtils;

import java.util.LinkedList;

public class HdfGroup implements HdfDataObject, HdfBTreeNode {
    private final String name;
    private final LinkedList<HdfBTreeNode> children;
    private final AllocationRecord localHeapAllocation;
    private final AllocationRecord localHeapDataAllocation;

    public HdfGroup(
            String name,
            LinkedList<HdfBTreeNode> children
    ) {
        this.name = name;
        this.children = children;
        this.localHeapAllocation = null;
        localHeapDataAllocation = null;
//        this.localHeapAllocation = new AllocationRecord(AllocationType.LOCAL_HEAP_HEADER, name,
//                HdfWriteUtils.hdfFixedPointFromValue(heapOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
//                hdfDataFile.getFileAllocation().HDF_LOCAL_HEAP_HEADER_SIZE,
//                hdfDataFile.getFileAllocation());
//        this.allocationRecord = new AllocationRecord(
//                AllocationType.LOCAL_HEAP, objectName + ":Local Heap Data", heapContentsOffset, heapContentsSize, hdfDataFile.getFileAllocation()
//        );
//        this.allocationRecord = new AllocationRecord(
//                AllocationType.LOCAL_HEAP, "LOCALHEAP2", offset, size, hdfDataFile.getFileAllocation()
//        );
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isGroup() {
        return true;
    }

    @Override
    public boolean isDataset() {
        return false;
    }
}
