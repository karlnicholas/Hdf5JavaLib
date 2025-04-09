package org.hdf5javalib;

import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

public interface HdfDataFile {
    HdfGlobalHeap getGlobalHeap();
    HdfFileAllocation getFileAllocation();
}
