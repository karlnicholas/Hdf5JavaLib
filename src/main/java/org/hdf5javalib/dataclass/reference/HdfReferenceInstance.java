package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDataHolder;

public interface HdfReferenceInstance {
    HdfDataHolder getData(HdfDataFile dataFile);
}
