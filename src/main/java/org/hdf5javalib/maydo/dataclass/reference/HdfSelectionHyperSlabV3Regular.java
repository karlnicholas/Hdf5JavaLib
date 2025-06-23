package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.hdffile.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.infrastructure.HdfDataObject;
import org.hdf5javalib.maydo.utils.HdfDataHolder;

public class HdfSelectionHyperSlabV3Regular extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int flags;
    private final int encodeSize;
    private final int rank;
    private final long[] start;
    private final long[] stride;
    private final long[] count;
    private final long[] block;

    public HdfSelectionHyperSlabV3Regular(int version, int flags, int encodeSize, int rank, long[] start, long[] stride, long[] count, long[] block) {
        this.version = version;
        this.flags = flags;
        this.encodeSize = encodeSize;
        this.rank = rank;
        this.start = start;
        this.stride = stride;
        this.count = count;
        this.block = block;
    }

    @Override
    public HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) {
        return null;
    }
}
