package org.hdf5javalib.maydo.hdfjava;

import java.util.ArrayList;
import java.util.List;

/**
 * A node in the B-Tree. It holds a list of BTreeObjects.
 */
public class HdfBTreeNode {
    // A node contains a list of named objects (Datasets or Groups).
    final List<HdfDataObject> entries;
    final int groupInternalNodeK;

    public HdfBTreeNode(int groupInternalNodeK) {
        this.groupInternalNodeK = groupInternalNodeK;
        this.entries = new ArrayList<>();
    }

    public boolean isLeaf() {
        for (HdfDataObject obj : entries) {
            if (obj instanceof HdfGroupObject) {
                if (!((HdfGroupObject) obj).getChildren().isEmpty()) {
                    return false; // If any group has children, this node is not a leaf.
                }
            }
        }
        return true;
    }

    public boolean isFull() {
        return entries.size() == 2 * groupInternalNodeK - 1;
    }

    // Helper to find an object by name within this node's entries.
    public HdfDataObject findObjectByName(String name) {
        for (HdfDataObject obj : entries) {
            if (obj.getObjectName().equals(name)) {
                return obj;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return "Node" + entries;
    }
}