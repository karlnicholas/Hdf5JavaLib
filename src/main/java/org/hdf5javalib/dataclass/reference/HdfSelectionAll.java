package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataObject;

public class HdfSelectionAll extends HdfDataspaceSelectionInstance {
    private final int version;

    public HdfSelectionAll(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "HdfSelectionAll{v=" + version + "}";
    }

    @Override
    public HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) {
        return null;
    }
}
