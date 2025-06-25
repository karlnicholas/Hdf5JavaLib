package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DataLayoutMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.ObjectHeaderContinuationMessage;
import org.hdf5javalib.maydo.utils.HdfWriteUtils;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;

public class HdfDataset implements HdfDataObject {
    private final String name;
    private final HdfObjectHeaderPrefix objectHeaderPrefix;
    protected final AllocationRecord dataObjectAllocationRecord;
    protected final AllocationRecord dataObjectContinuationAllocationRecord;
    protected final AllocationRecord dataAllocationRecord;

    public HdfDataset(
            String name,
            HdfObjectHeaderPrefix objectHeaderPrefix,
            HdfFixedPoint offset,
            long objectHeaderSize,
            HdfDataFile hdfDataFile,
            int OBJECT_HREADER_PREFIX_HEADER_SIZE

    ) {
        this.name = name;
        this.objectHeaderPrefix = objectHeaderPrefix;
        this.dataObjectAllocationRecord = new AllocationRecord(
                AllocationType.DATASET_OBJECT_HEADER,
                name + ":Object Header",
                offset,
                HdfWriteUtils.hdfFixedPointFromValue(
                        objectHeaderSize + OBJECT_HREADER_PREFIX_HEADER_SIZE,
                        hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength()
                ), hdfDataFile.getFileAllocation()
        );
        this.dataObjectContinuationAllocationRecord = objectHeaderPrefix.findMessageByType(ObjectHeaderContinuationMessage.class)
                .map(cm->
                        new AllocationRecord(
                                AllocationType.DATASET_HEADER_CONTINUATION,
                                name+ ":Header Continuation",
                                cm.getContinuationOffset(),
                                cm.getContinuationSize(),
                                hdfDataFile.getFileAllocation())
                ).orElse(null);
        this.dataAllocationRecord = objectHeaderPrefix.findMessageByType(DataLayoutMessage.class)
                .map(dlm->
                        dlm.getDataAddress().isUndefined() ? null :
                                new AllocationRecord(
                                        AllocationType.DATASET_DATA,
                                        name+ ":Data",
                                        dlm.getDataAddress(),
                                        dlm.getDimensionSizes()[0],
                                        hdfDataFile.getFileAllocation())
                ).orElse(null);

    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isGroup() {
        return false;
    }

    @Override
    public boolean isDataset() {
        return false;
    }

    public AllocationRecord getDataObjectAllocationRecord() {
        return dataObjectAllocationRecord;
    }

}
