package org.hdf5javalib.hdfjava;

import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;

/**
 * Defines the contract for any node in the B-Tree.
 * Nodes are comparable based on their name.
 */
public interface HdfBTreeNode extends Comparable<HdfBTreeNode> {

    String getObjectName();

    String getHardLink();

    HdfObjectHeaderPrefix getObjectHeader();

    int getLevel();

    HdfBTreeNode getParent();

    void setParent(HdfBTreeNode parent);

    boolean isDataset();

    HdfDataObject getDataObject();

    String getObjectPath();

}