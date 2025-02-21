package com.github.karlnicholas.hdf5javalib;

import java.util.ArrayList;
import java.util.List;

public class GroupSymbolTableNode {
    private final boolean isLeaf;
    private List<GroupSymbolTableNode> childNodes;
    private List<SymbolTableEntry> entries;

    public GroupSymbolTableNode(boolean isLeaf) {
        this.isLeaf = isLeaf;
        if (isLeaf) {
            entries = new ArrayList<>();
        } else {
            childNodes = new ArrayList<>();
        }
    }

    public boolean isLeaf() { return isLeaf; }

    public List<GroupSymbolTableNode> getChildNodes() { return childNodes; }
    public List<SymbolTableEntry> getEntries() { return entries; }

    public void addEntry(SymbolTableEntry entry) {
        if (isLeaf) {
            entries.add(entry);
        }
    }

    public void addChild(GroupSymbolTableNode child) {
        if (!isLeaf) {
            childNodes.add(child);
        }
    }

    @Override
    public String toString() {
        return isLeaf ? "Leaf: " + entries.toString() : "Internal: " + childNodes.toString();
    }
}
