package org.hdf5javalib.hdfjava;

import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;

/**
 * Defines the contract for any node in the B-Tree.
 * Nodes are comparable based on their name.
 */
public interface HdfTreeNode extends Comparable<HdfTreeNode> {

    String getObjectName();

    String getHardLink();

    HdfObjectHeaderPrefix getObjectHeader();

    int getLevel();

    HdfTreeNode getParent();

    void setParent(HdfTreeNode parent);

    boolean isDataset();

    HdfDataObject getDataObject();

    String getObjectPath();

}