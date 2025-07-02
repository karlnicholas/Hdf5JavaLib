package org.hdf5javalib.maydo.hdfjava;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Represents a group entry (a "directory") in the tree.
 * It can contain a list of child BTreeNodes.
 */
public class HdfGroupObject extends HdfDataObject {
    // This is the list of children BTreeNodes that "live inside" this group.
    private final List<HdfBTreeNode> children = new ArrayList<>();

    public HdfGroupObject(String name) {
        super(name);
    }

    @Override
    public Optional<HdfBTree> getBTreeOptionally() {
        return Optional.empty();
    }

    public List<HdfBTreeNode> getChildren() {
        return children;
    }

    @Override
    public String toString() {
        return String.format("Group(name=%s, children_count=%d)", objectName, children.size());
    }
}