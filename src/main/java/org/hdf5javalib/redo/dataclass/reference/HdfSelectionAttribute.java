package org.hdf5javalib.redo.dataclass.reference;

import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.HdfDataObject;
import org.hdf5javalib.redo.utils.HdfDataHolder;

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
