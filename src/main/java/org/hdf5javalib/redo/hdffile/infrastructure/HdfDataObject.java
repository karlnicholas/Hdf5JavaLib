package org.hdf5javalib.redo.hdffile.infrastructure;

import java.util.Optional;

public interface HdfDataObject {
    String getObjectName();

    Optional<HdfBTreeV1> getBTreeOptionally(); // Datasets have no B-tree
}
