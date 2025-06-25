package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfDataHolder;

public interface HdfReferenceInstance {
    HdfDataHolder getData(HdfDataFile dataFile);
}
