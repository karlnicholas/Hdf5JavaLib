package org.hdf5javalib.hdfjava;

import java.util.*;
import java.util.logging.Logger;

/**
 * Represents the B-Tree itself. It is iterable and provides specialized
 * iterators for all nodes or just for datasets.
 */
public class HdfTree implements Iterable<HdfTreeNode> {
    private static final Logger log = Logger.getLogger(HdfTree.class.getName());
    private final HdfGroup root;

    public HdfTree(HdfGroup root) {
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
    public Iterator<HdfTreeNode> iterator() {
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

    public Optional<HdfTreeNode> findByPath(String path) {
        // ... (findByPath method remains the same as before) ...
        if (path == null || !path.startsWith("/")) {
            throw new IllegalArgumentException("Path must be non-null and start with '/'");
        }

        if (path.equals("/")) {
            return Optional.of(this.root);
        }

        String[] names = path.substring(1).split("/");
        HdfTreeNode currentNode = this.root;

        for (String name : names) {
            if (name.isEmpty()) continue;

            if (!(currentNode instanceof HdfGroup currentGroup)) {
                return Optional.empty();
            }

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

    private void printNode(HdfTreeNode node, String prefix, boolean isTail) {
        log.info(prefix + (isTail ? "└── " : "├── ") + node + " (Level: " + node.getLevel() + ")");
        if (node instanceof HdfGroup group) {
            List<HdfTreeNode> children = group.getChildren();
            for (int i = 0; i < children.size() - 1; i++) {
                printNode(children.get(i), prefix + (isTail ? "    " : "│   "), false);
            }
            if (!children.isEmpty()) {
                printNode(children.get(children.size() - 1), prefix + (isTail ? "    " : "│   "), true);
            }
        }
    }

    // BTreeIterator with Deque
    private static class BTreeIterator implements Iterator<HdfTreeNode> {
        private final Deque<HdfTreeNode> stack = new ArrayDeque<>();

        public BTreeIterator(HdfTreeNode root) {
            if (root != null) {
                stack.push(root);
            }
        }

        @Override
        public boolean hasNext() {
            return !stack.isEmpty();
        }

        @Override
        public HdfTreeNode next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more nodes in the tree.");
            }

            HdfTreeNode currentNode = stack.pop();

            if (currentNode instanceof HdfGroup group) {
                List<HdfTreeNode> children = group.getChildren();
                for (int i = children.size() - 1; i >= 0; i--) {
                    stack.push(children.get(i));
                }
            }
            return currentNode;
        }
    }

    // BTreeDatasetIterator with Deque
    private static class BTreeDatasetIterator implements Iterator<HdfDataset> {
        private final Deque<HdfTreeNode> stack = new ArrayDeque<>();
        private HdfDataset nextHdfDataset;

        public BTreeDatasetIterator(HdfTreeNode root) {
            if (root != null) {
                stack.push(root);
            }
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
            findNext();
            return hdfDatasetToReturn;
        }

        private void findNext() {
            while (!stack.isEmpty()) {
                HdfTreeNode currentNode = stack.pop();

                if (currentNode instanceof HdfGroup group) {
                    List<HdfTreeNode> children = group.getChildren();
                    for (int i = children.size() - 1; i >= 0; i--) {
                        stack.push(children.get(i));
                    }
                } else if (currentNode instanceof HdfDataset) {
                    this.nextHdfDataset = (HdfDataset) currentNode;
                    return;
                }
            }
            this.nextHdfDataset = null;
        }
    }
}