package org.hdf5javalib.redo.reference;

public class HdfSelectionAll extends HdfDataspaceSelectionInstance {
    private final int version;
    public HdfSelectionAll(int version) {
        this.version = version;
    }
    @Override
    public String toString() {
        return "HdfSelectPoints{v=" + version + "}";
    }
}
