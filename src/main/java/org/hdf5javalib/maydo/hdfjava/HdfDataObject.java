package org.hdf5javalib.maydo.hdfjava;

import java.util.Objects;
import java.util.Optional;

/**
 * Abstract base class for all objects stored in the B-Tree,
 * providing a common 'name' property for ordering and identification.
 */
public abstract class HdfDataObject implements Comparable<HdfDataObject> {
    protected final String objectName;

    public HdfDataObject(String objectName) {
        this.objectName = Objects.requireNonNull(objectName, "objectName cannot be null");
    }

    public String getObjectName() {
        return objectName;
    }

    @Override
    public int compareTo(HdfDataObject other) {
        return this.objectName.compareTo(other.objectName);
    }

    public abstract Optional<HdfBTree> getBTreeOptionally();
}