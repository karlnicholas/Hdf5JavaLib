package org.hdf5javalib.redo.hdfjava;

public class HdfDataset implements HdfDataObject {
    private final String name;

    public HdfDataset(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isGroup() {
        return false;
    }

    @Override
    public boolean isDataset() {
        return false;
    }
}
