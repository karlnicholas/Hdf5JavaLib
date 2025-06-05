package org.hdf5javalib.redo.reference;

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
}
