package org.hdf5javalib.redo.hdfjava;

public interface HdfDataObject {
    String getName();
    boolean isGroup();
    boolean isDataset();
}
