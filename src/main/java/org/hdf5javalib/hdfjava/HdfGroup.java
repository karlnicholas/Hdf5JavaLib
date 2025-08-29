package org.hdf5javalib.hdfjava;

import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.util.*;
import java.util.function.Function;

public class HdfGroup extends HdfDataObject {

    private final List<HdfTreeNode> children;

    public HdfGroup(String name, HdfObjectHeaderPrefix objectHeader, HdfTreeNode parent, String hardLink) {
        super(name, objectHeader, parent, hardLink);
        this.children = new ArrayList<>();
    }

    public void visitAllNodes(Function<HdfTreeNode, Boolean> visitor) {
        Queue<HdfTreeNode> queue = new LinkedList<>();
        queue.add(this);

        while (!queue.isEmpty()) {
            HdfTreeNode current = queue.poll();
            if ( visitor.apply(current) ) {
                return;
            };

            if (current instanceof HdfGroup group) {
                for (HdfTreeNode child : group.children) {
                    queue.add(child);
                }
            }
        }
    }

    public HdfTreeNode getRoot() {
        HdfTreeNode current = this;
        while (current.getParent() != null) {
            current = current.getParent();
        }
        return current;
    }

    /**
     * Adds a child node, maintaining the sorted order of the children list.
     * Prevents adding children with duplicate names.
     *
     * @param child The HdfTreeNode to add as a child.
     * @throws IllegalArgumentException if a child with the same name already exists.
     */
    public void addChild(HdfTreeNode child) {
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
    public Optional<HdfTreeNode> findChildByName(String name) {
        // To use binarySearch, we need a "key" object of the same type.
        // We can create a temporary, lightweight HdfDataset object for this purpose.
        // The value doesn't matter, as the comparison is only on the name.
        HdfTreeNode searchKey = new HdfDataset(name, null, null, null);

        int index = Collections.binarySearch(children, searchKey);

        if (index >= 0) {
            return Optional.of(children.get(index));
        } else {
            return Optional.empty(); // Not found
        }
    }

    public List<HdfTreeNode> getChildren() {
        return Collections.unmodifiableList(children);
    }

    // getLevel() method remains the same
    @Override
    public int getLevel() {
        if (children.isEmpty()) {
            return 0;
        }
        int maxChildLevel = children.stream()
                .mapToInt(HdfTreeNode::getLevel)
                .max()
                .orElse(-1);
        return 1 + maxChildLevel;
    }

    @Override
    public boolean isDataset() {
        return false;
    }
}