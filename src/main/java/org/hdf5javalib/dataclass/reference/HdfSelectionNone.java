package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataObject;
import org.hdf5javalib.utils.HdfDataHolder;

public class HdfSelectionNone extends HdfDataspaceSelectionInstance {
    private final int version;

    public HdfSelectionNone(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "HdfSelectionNone{v=" + version + "}";
    }

    @Override
    public HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) {
        return null;
    }
}
