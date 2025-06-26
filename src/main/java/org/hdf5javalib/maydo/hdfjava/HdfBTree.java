package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;

public class HdfBTree {
    private final AllocationRecord allocationRecord;

    public HdfBTree(AllocationRecord allocationRecord) {
        this.allocationRecord = allocationRecord;
//        this.allocationRecord = new AllocationRecord(AllocationType.BTREE_HEADER, name, offset,
//                new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
//                hdfDataFile.getFileAllocation()
//        );
//        this.allocationRecord = new AllocationRecord(AllocationType.BTREE_HEADER, name, offset,
//                new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
//                hdfDataFile.getFileAllocation()
//        );
    }
    public AllocationRecord getAllocationRecord() {
        return allocationRecord;
    }

}
