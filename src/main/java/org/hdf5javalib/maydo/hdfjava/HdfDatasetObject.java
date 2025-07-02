package org.hdf5javalib.maydo.hdfjava;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a data entry (a "file") in the tree.
 * It holds a BigInteger value and has no children.
 */
public class HdfDatasetObject extends HdfDataObject {
    private final BigInteger value;

    public HdfDatasetObject(String name, BigInteger value) {
        super(name);
        this.value = Objects.requireNonNull(value, "BigInteger value cannot be null.");
    }

    public BigInteger getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("Dataset(name=%s, value=%s)", objectName, value);
    }

    @Override
    public Optional<HdfBTree> getBTreeOptionally() {
        return Optional.empty();
    }
}