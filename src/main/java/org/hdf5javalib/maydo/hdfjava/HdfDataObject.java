package org.hdf5javalib.maydo.hdfjava;

public interface HdfDataObject {
    String getName();
    boolean isGroup();
    boolean isDataset();
}
