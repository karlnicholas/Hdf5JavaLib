package org.hdf5javalib.hdfjava;

import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.util.Objects;

/**
 * An abstract base class for HdfTreeNode implementations, providing common
 * properties and the comparison logic based on the node's name.
 */
public abstract class HdfDataObject implements HdfTreeNode {

    protected final String objectName;
    protected final HdfObjectHeaderPrefix objectHeader;
    protected HdfTreeNode parent;
    protected final String hardLink;

    public HdfDataObject(String objectName, HdfObjectHeaderPrefix objectHeader, HdfTreeNode parent, String hardLink) {
        this.objectName = Objects.requireNonNull(objectName, "Node name cannot be null.");
        if (objectName.contains("/")) {
            throw new IllegalArgumentException("Node name cannot contain '/' character.");
        }
        this.objectHeader = objectHeader;
        this.parent = parent;
        this.hardLink = hardLink;
    }

    // other getters and setters remain the same...
    @Override
    public String getObjectName() { return objectName; }
    @Override
    public HdfObjectHeaderPrefix getObjectHeader() { return objectHeader; }
    @Override
    public HdfTreeNode getParent() { return parent; }
    @Override
    public void setParent(HdfTreeNode parent) { this.parent = parent; }
    @Override
    public String getObjectPath() {
        StringBuilder path = new StringBuilder(Objects.requireNonNull(objectName, "Node name cannot be null."));
        while(parent!=null) {
            path.insert(0, '/');
            path.insert(0, parent.getObjectName());
            parent=parent.getParent();
        }
        path.insert(0, '/');
        return path.toString();
    }

    public String getHardLink() { return hardLink; }
    /**
     * Compares this node to another node based on their names.
     * This is essential for sorting and binary searching.
     *
     * @param other The other node to compare against.
     * @return a negative integer, zero, or a positive integer as this node's name
     *         is less than, equal to, or greater than the specified node's name.
     */
    @Override
    public final int compareTo(HdfTreeNode other) {
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