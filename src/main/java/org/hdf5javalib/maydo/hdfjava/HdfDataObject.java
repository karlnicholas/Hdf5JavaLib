package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.util.Objects;
import java.util.Optional;

/**
 * Abstract base class for all objects stored in the B-Tree,
 * providing a common 'name' property for ordering and identification.
 */
public abstract class HdfDataObject implements Comparable<HdfDataObject> {
    protected final String objectName;
    private final HdfObjectHeaderPrefix objectHeader;

    public HdfDataObject(String objectName, HdfObjectHeaderPrefix objectHeader) {
        this.objectName = Objects.requireNonNull(objectName, "objectName cannot be null");
        this.objectHeader = Objects.requireNonNull(objectHeader, "objectHeader cannot be null");
    }

    public String getObjectName() {
        return objectName;
    }

    public HdfObjectHeaderPrefix getObjectHeader() {
        return objectHeader;
    }

    @Override
    public int compareTo(HdfDataObject other) {
        return this.objectName.compareTo(other.objectName);
    }

    public abstract Optional<HdfBTree> getBTreeOptionally();
}