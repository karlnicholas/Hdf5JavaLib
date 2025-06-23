package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.hdffile.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.infrastructure.HdfDataObject;
import org.hdf5javalib.maydo.utils.HdfDataHolder;

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
