package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class HdfGroup extends HdfDataObject {

    private final List<HdfBTreeNode> children;

    public HdfGroup(String name, HdfObjectHeaderPrefix objectHeader) {
        super(name, objectHeader);
        this.children = new ArrayList<>();
    }

    /**
     * Adds a child node, maintaining the sorted order of the children list.
     * Prevents adding children with duplicate names.
     *
     * @param child The HdfBTreeNode to add as a child.
     * @throws IllegalArgumentException if a child with the same name already exists.
     */
    public void addChild(HdfBTreeNode child) {
        if (child == null) {
            return;
        }

        // Use binary search to find the correct insertion point or check for duplicates.
        int index = Collections.binarySearch(children, child);

        if (index >= 0) {
            // A child with the same name already exists.
            throw new IllegalArgumentException(
                    "A child with the name '" + child.getObjectName() + "' already exists in this group."
            );
        }

        // `binarySearch` returns `(-(insertion point) - 1)` if not found.
        int insertionPoint = -(index + 1);
        children.add(insertionPoint, child);
        child.setParent(this);
    }

    /**
     * Finds a direct child by its name using an efficient binary search.
     *
     * @param name The name of the child to find.
     * @return An Optional containing the found node, or an empty Optional.
     */
    public Optional<HdfBTreeNode> findChildByName(String name) {
        // To use binarySearch, we need a "key" object of the same type.
        // We can create a temporary, lightweight HdfDataset object for this purpose.
        // The value doesn't matter, as the comparison is only on the name.
        HdfBTreeNode searchKey = new HdfDataset(name, null);

        int index = Collections.binarySearch(children, searchKey);

        if (index >= 0) {
            return Optional.of(children.get(index));
        } else {
            return Optional.empty(); // Not found
        }
    }

    public List<HdfBTreeNode> getChildren() {
        return Collections.unmodifiableList(children);
    }

    // getLevel() method remains the same
    @Override
    public int getLevel() {
        if (children.isEmpty()) {
            return 0;
        }
        int maxChildLevel = children.stream()
                .mapToInt(HdfBTreeNode::getLevel)
                .max()
                .orElse(-1);
        return 1 + maxChildLevel;
    }

    @Override
    public boolean isDataset() {
        return false;
    }
}