package org.hdf5javalib.maydo.hdfjava;

import java.util.Optional;

public interface HdfDataObject {
    String getObjectName();

    Optional<HdfBTree> getBTreeOptionally(); // Datasets have no B-tree
}
