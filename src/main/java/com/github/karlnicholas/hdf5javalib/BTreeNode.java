package com.github.karlnicholas.hdf5javalib;

import java.util.ArrayList;
import java.util.List;

public class BTreeNode {
    List<Integer> keys;
    List<Object> children; // Can store either BTreeNode or GroupSymbolTableNode
    boolean isLeaf;
    int maxKeys; // Maximum keys before splitting

    public BTreeNode(boolean isLeaf, int maxKeys) {
        this.isLeaf = isLeaf;
        this.maxKeys = maxKeys;
        keys = new ArrayList<>();
        children = new ArrayList<>();
    }
}
