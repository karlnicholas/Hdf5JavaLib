package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a data entry (a "file") in the tree.
 * It holds a BigInteger value and has no children.
 */
public class HdfDatasetObject extends HdfDataObject {

    public HdfDatasetObject(String name, HdfObjectHeaderPrefix objectHeader) {
        super(name,   objectHeader);
    }

    @Override
    public String toString() {
        return String.format("Dataset(name=%s, value=%s)", objectName);
    }

    @Override
    public Optional<HdfBTree> getBTreeOptionally() {
        return Optional.empty();
    }
}