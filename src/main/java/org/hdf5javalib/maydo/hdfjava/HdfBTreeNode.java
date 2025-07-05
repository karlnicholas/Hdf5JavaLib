package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.math.BigInteger;

/**
 * Defines the contract for any node in the B-Tree.
 * Nodes are comparable based on their name.
 */
public interface HdfBTreeNode extends Comparable<HdfBTreeNode> {

    String getObjectName();

    HdfObjectHeaderPrefix getObjectHeader();

    int getLevel();

    HdfBTreeNode getParent();

    void setParent(HdfBTreeNode parent);

    boolean isDataset();
}