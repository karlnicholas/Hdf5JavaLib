package org.hdf5javalib.redo.dataclass.reference;

public class HdfSelectionAll extends HdfDataspaceSelectionInstance {
    private final int version;

    public HdfSelectionAll(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "HdfSelectPointsV1{v=" + version + "}";
    }
}
