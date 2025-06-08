package org.hdf5javalib.redo.dataclass.reference;

public class HdfSelectionNone extends HdfDataspaceSelectionInstance {
    private final int version;

    public HdfSelectionNone(int version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "HdfSelectPointsV1{v=" + version + "}";
    }
}
