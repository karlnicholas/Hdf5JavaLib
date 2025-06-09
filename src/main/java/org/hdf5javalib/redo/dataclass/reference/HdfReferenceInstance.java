package org.hdf5javalib.redo.dataclass.reference;

import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.utils.HdfDataHolder;

public interface HdfReferenceInstance {
    HdfDataHolder getData(HdfDataFile dataFile);
}
