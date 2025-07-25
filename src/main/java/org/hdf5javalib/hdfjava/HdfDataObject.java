package org.hdf5javalib.hdfjava;

import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.util.Objects;

/**
 * An abstract base class for HdfBTreeNode implementations, providing common
 * properties and the comparison logic based on the node's name.
 */
public abstract class HdfDataObject implements HdfBTreeNode {

    protected final String objectName;
    protected final HdfObjectHeaderPrefix objectHeader;
    protected HdfBTreeNode parent;

    public HdfDataObject(String objectName, HdfObjectHeaderPrefix objectHeader, HdfBTreeNode parent) {
        this.objectName = Objects.requireNonNull(objectName, "Node name cannot be null.");
        if (objectName.contains("/")) {
            throw new IllegalArgumentException("Node name cannot contain '/' character.");
        }
        this.objectHeader = objectHeader;
        this.parent = parent;
    }

    // other getters and setters remain the same...
    @Override
    public String getObjectName() { return objectName; }
    @Override
    public HdfObjectHeaderPrefix getObjectHeader() { return objectHeader; }
    @Override
    public HdfBTreeNode getParent() { return parent; }
    @Override
    public void setParent(HdfBTreeNode parent) { this.parent = parent; }
    @Override
    public String getObjectPath() {
        StringBuilder path = new StringBuilder(Objects.requireNonNull(objectName, "Node name cannot be null."));
        while(parent!=null) {
            path.insert(0, '/');
            path.insert(0, parent.getObjectName());
            parent=parent.getParent();
        }
        return path.toString();
    }

    /**
     * Compares this node to another node based on their names.
     * This is essential for sorting and binary searching.
     *
     * @param other The other node to compare against.
     * @return a negative integer, zero, or a positive integer as this node's name
     *         is less than, equal to, or greater than the specified node's name.
     */
    @Override
    public final int compareTo(HdfBTreeNode other) {
        return this.objectName.compareTo(other.getObjectName());
    }

    @Override
    public String toString() {
        String displayName = objectName.isEmpty() ? "/" : objectName;
        return String.format("%s[name='%s', value=%s]", this.getClass().getSimpleName(), displayName, objectHeader);
    }

    @Override
    public HdfDataObject getDataObject() {
        return this;
    }
}