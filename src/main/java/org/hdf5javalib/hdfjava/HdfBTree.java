package org.hdf5javalib.hdfjava;

import java.util.*;

/**
 * Represents the B-Tree itself. It is iterable and provides specialized
 * iterators for all nodes or just for datasets.
 */
public class HdfBTree implements Iterable<HdfBTreeNode> {

    private final HdfGroup root;

    public HdfBTree(HdfGroup root) {
        if (!root.getObjectName().isEmpty()) {
            throw new IllegalArgumentException("Root node name must be an empty string \"\".");
        }
        this.root = root;
    }

    public HdfGroup getRoot() {
        return root;
    }

    /**
     * Returns an iterator over all nodes (Groups and Datasets) in the tree.
     * The iteration order is pre-order depth-first.
     *
     * @return An iterator for all BTreeNodes.
     */
    @Override
    public Iterator<HdfBTreeNode> iterator() {
        return new BTreeIterator(root);
    }

    /**
     * Returns a specialized iterator that only iterates over the HdfDataset nodes.
     * The traversal order is still pre-order depth-first.
     *
     * @return An iterator for Datasets.
     */
    public Iterator<HdfDataset> datasetIterator() {
        return new BTreeDatasetIterator(root);
    }

    public Optional<HdfBTreeNode> findByPath(String path) {
        // ... (findByPath method remains the same as before) ...
        if (path == null || !path.startsWith("/")) {
            throw new IllegalArgumentException("Path must be non-null and start with '/'");
        }

        if (path.equals("/")) {
            return Optional.of(this.root);
        }

        String[] names = path.substring(1).split("/");
        HdfBTreeNode currentNode = this.root;

        for (String name : names) {
            if (name.isEmpty()) continue;

            if (!(currentNode instanceof HdfGroup)) {
                return Optional.empty();
            }

            HdfGroup currentGroup = (HdfGroup) currentNode;
            currentNode = currentGroup.findChildByName(name).orElse(null);

            if (currentNode == null) {
                return Optional.empty();
            }
        }
        return Optional.of(currentNode);
    }

    public void printTree() {
        // ... (printTree method remains the same as before) ...
        printNode(root, "", true);
    }

    private void printNode(HdfBTreeNode node, String prefix, boolean isTail) {
        System.out.println(prefix + (isTail ? "└── " : "├── ") + node + " (Level: " + node.getLevel() + ")");
        if (node instanceof HdfGroup) {
            HdfGroup group = (HdfGroup) node;
            List<HdfBTreeNode> children = group.getChildren();
            for (int i = 0; i < children.size() - 1; i++) {
                printNode(children.get(i), prefix + (isTail ? "    " : "│   "), false);
            }
            if (!children.isEmpty()) {
                printNode(children.get(children.size() - 1), prefix + (isTail ? "    " : "│   "), true);
            }
        }
    }

    // --- Inner Iterator Classes ---

    /**
     * An iterator that performs a pre-order, depth-first traversal of all nodes.
     * It uses a stack to manage the traversal state without recursion.
     */
    private static class BTreeIterator implements Iterator<HdfBTreeNode> {
        private final Stack<HdfBTreeNode> stack = new Stack<>();

        public BTreeIterator(HdfBTreeNode root) {
            if (root != null) {
                stack.push(root);
            }
        }

        @Override
        public boolean hasNext() {
            return !stack.isEmpty();
        }

        @Override
        public HdfBTreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more nodes in the tree.");
            }

            HdfBTreeNode currentNode = stack.pop();

            // If the current node is a group, push its children onto the stack
            // in reverse order to maintain the correct pre-order traversal (left to right).
            if (currentNode instanceof HdfGroup) {
                HdfGroup group = (HdfGroup) currentNode;
                List<HdfBTreeNode> children = group.getChildren();
                for (int i = children.size() - 1; i >= 0; i--) {
                    stack.push(children.get(i));
                }
            }
            return currentNode;
        }
    }

    /**
     * An iterator that traverses all nodes but only returns HdfDataset instances.
     * It uses a "look-ahead" approach to find the next dataset.
     */
    private static class BTreeDatasetIterator implements Iterator<HdfDataset> {
        private final Stack<HdfBTreeNode> stack = new Stack<>();
        private HdfDataset nextHdfDataset;

        public BTreeDatasetIterator(HdfBTreeNode root) {
            if (root != null) {
                stack.push(root);
            }
            // Find the first dataset to prime the iterator
            findNext();
        }

        @Override
        public boolean hasNext() {
            return nextHdfDataset != null;
        }

        @Override
        public HdfDataset next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more datasets in the tree.");
            }
            HdfDataset hdfDatasetToReturn = nextHdfDataset;
            // Look for the next dataset for the *next* call to next()
            findNext();
            return hdfDatasetToReturn;
        }

        /**
         * A helper method to find the next HdfDataset and store it in the nextHdfDataset field.
         */
        private void findNext() {
            while (!stack.isEmpty()) {
                HdfBTreeNode currentNode = stack.pop();

                // If it's a group, push its children for future traversal
                if (currentNode instanceof HdfGroup) {
                    HdfGroup group = (HdfGroup) currentNode;
                    List<HdfBTreeNode> children = group.getChildren();
                    for (int i = children.size() - 1; i >= 0; i--) {
                        stack.push(children.get(i));
                    }
                }
                // If it's a dataset, we've found our next item. Store it and break.
                else if (currentNode instanceof HdfDataset) {
                    this.nextHdfDataset = (HdfDataset) currentNode;
                    return;
                }
            }
            // If the loop finishes, there are no more datasets
            this.nextHdfDataset = null;
        }
    }
}