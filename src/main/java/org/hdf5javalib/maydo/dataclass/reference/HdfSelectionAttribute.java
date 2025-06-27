package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.hdfjava.HdfDataObject;
import org.hdf5javalib.maydo.utils.HdfDataHolder;

public class HdfSelectionAttribute extends HdfDataspaceSelectionInstance {
    private final String attributeName;

    public HdfSelectionAttribute(String attributeName) {
        this.attributeName = attributeName;
    }

    @Override
    public String toString() {
        return "HdfSelectionAttribute{a=" + attributeName + "}";
    }

    public String getAttributeName() {
        return attributeName;
    }

    @Override
    public HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) {
        return null;
    }
}
