package org.hdf5javalib.maydo.hdfjava;

import java.util.LinkedList;

public class HdfGroup implements HdfDataObject, HdfBTreeNode {
    private final String name;
    private final LinkedList<HdfBTreeNode> children;

    public HdfGroup(
            String name,
            LinkedList<HdfBTreeNode> children
    ) {
        this.name = name;
        this.children = children;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isGroup() {
        return true;
    }

    @Override
    public boolean isDataset() {
        return false;
    }
}
