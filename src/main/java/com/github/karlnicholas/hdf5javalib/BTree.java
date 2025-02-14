package com.github.karlnicholas.hdf5javalib;

import java.util.ArrayList;
import java.util.List;

/**
 * Implements an HDF5 B-Tree for indexing symbol table entries and metadata.
 */
public class BTree {
    private BTreeNode root;
    private final int leafNodeMaxKeys;
    private final int internalNodeMaxKeys;
    private final SymbolTableEntry rootEntry;

    public BTree(int groupLeafNodeK, int groupInternalNodeK, SymbolTableEntry rootEntry) {
        this.leafNodeMaxKeys = groupLeafNodeK;
        this.internalNodeMaxKeys = groupInternalNodeK;
        this.rootEntry = rootEntry;
        this.root = new BTreeNode(false, internalNodeMaxKeys); // Root starts as an internal node
        initializeRoot();
    }

    private void initializeRoot() {
        root.keys.add(rootEntry.getSymbol().hashCode());

        GroupSymbolTableNode rootLeaf = new GroupSymbolTableNode(true);
        rootLeaf.addEntry(rootEntry);

        root.children.add(rootLeaf);
    }

    public void insert(int key, SymbolTableEntry entry) {
        if (entry.getSymbol().equals(rootEntry.getSymbol())) {
            throw new IllegalArgumentException("Error: Root entry cannot be modified.");
        }

        if (root.keys.size() == internalNodeMaxKeys) {
            BTreeNode newRoot = new BTreeNode(false, internalNodeMaxKeys);
            newRoot.children.add(root);
            splitChild(newRoot, 0);
            root = newRoot;
        }

        insertNonFull(root, key, entry);
    }

    private void insertNonFull(BTreeNode node, int key, SymbolTableEntry entry) {
        int i = node.keys.size() - 1;

        // **Check if node is a leaf node**
        if (node.isLeaf) {
            GroupSymbolTableNode leafNode = (GroupSymbolTableNode) node.children.get(0);

            if (leafNode.getEntries().size() == leafNodeMaxKeys) {
                splitLeaf(node, key, entry);
                return;
            }

            leafNode.addEntry(entry);
            node.keys.add(key);
        } else {
            // **Navigate to the correct child**
            while (i >= 0 && key < node.keys.get(i)) {
                i--;
            }
            i++;

            // **Ensure correct casting**
            Object child = node.children.get(i);
            if (child instanceof BTreeNode) {
                BTreeNode childNode = (BTreeNode) child;
                if (childNode.keys.size() == childNode.maxKeys) {
                    splitChild(node, i);
                    if (key > node.keys.get(i)) {
                        i++;
                    }
                }
                insertNonFull((BTreeNode) node.children.get(i), key, entry);
            } else if (child instanceof GroupSymbolTableNode) {
                GroupSymbolTableNode leafNode = (GroupSymbolTableNode) child;
                if (leafNode.getEntries().size() == leafNodeMaxKeys) {
                    splitLeaf(node, i, entry);
                } else {
                    leafNode.addEntry(entry);
                }
            }
        }
    }

    private void splitLeaf(BTreeNode parent, int index, SymbolTableEntry entry) {
        GroupSymbolTableNode leftLeaf = (GroupSymbolTableNode) parent.children.get(index);
        GroupSymbolTableNode rightLeaf = new GroupSymbolTableNode(true);

        int mid = leafNodeMaxKeys / 2;
        List<SymbolTableEntry> toMove = new ArrayList<>(leftLeaf.getEntries().subList(mid, leftLeaf.getEntries().size()));
        rightLeaf.getEntries().addAll(toMove);
        leftLeaf.getEntries().subList(mid, leftLeaf.getEntries().size()).clear();

        int separatorKey = rightLeaf.getEntries().get(0).getSymbol().hashCode();

        parent.keys.add(index, separatorKey);
        parent.children.add(index + 1, rightLeaf);

        if (entry.getSymbol().hashCode() < separatorKey) {
            leftLeaf.addEntry(entry);
        } else {
            rightLeaf.addEntry(entry);
        }

        if (parent == root && root.children.get(0) instanceof GroupSymbolTableNode) {
            upgradeRootToInternal(leftLeaf, rightLeaf);
        }
    }

    private void upgradeRootToInternal(GroupSymbolTableNode leftLeaf, GroupSymbolTableNode rightLeaf) {
        BTreeNode newInternalRoot = new BTreeNode(false, internalNodeMaxKeys);

        newInternalRoot.keys.add(root.keys.get(0));

        BTreeNode leftChild = new BTreeNode(true, leafNodeMaxKeys);
        leftChild.children.add(leftLeaf);

        BTreeNode rightChild = new BTreeNode(true, leafNodeMaxKeys);
        rightChild.children.add(rightLeaf);

        newInternalRoot.children.add(leftChild);
        newInternalRoot.children.add(rightChild);

        root = newInternalRoot;
    }

    private void splitChild(BTreeNode parent, int index) {
        BTreeNode child = (BTreeNode) parent.children.get(index);
        BTreeNode rightChild = new BTreeNode(child.isLeaf, child.maxKeys);
        int mid = child.maxKeys / 2;

        parent.keys.add(index, child.keys.get(mid));
        parent.children.add(index + 1, rightChild);

        rightChild.keys.addAll(new ArrayList<>(child.keys.subList(mid + 1, child.keys.size())));
        child.keys.subList(mid, child.keys.size()).clear();

        if (!child.isLeaf) {
            rightChild.children.addAll(new ArrayList<>(child.children.subList(mid + 1, child.children.size())));
            child.children.subList(mid + 1, child.children.size()).clear();
        }
    }

    public void traverse() {
        traverse(root);
    }

    private void traverse(BTreeNode node) {
        if (node != null) {
            System.out.print("Root: ");
            for (int key : node.keys) {
                System.out.print(key + " ");
            }
            System.out.println();

            for (Object child : node.children) {
                if (child instanceof BTreeNode) {
                    traverse((BTreeNode) child);
                } else if (child instanceof GroupSymbolTableNode) {
                    System.out.println("Leaf Node: " + child);
                }
            }
        }
    }

    public boolean isRootEntryModified() {
        SymbolTableEntry rootSymbolEntry = getRootSymbolTableEntry();
        return rootSymbolEntry == null || !rootSymbolEntry.getSymbol().equals(rootEntry.getSymbol());
    }

    public boolean isRootStillInternal() {
        return root.children.size() >= 2 && root.children.get(0) instanceof BTreeNode;
    }

    public SymbolTableEntry getRootSymbolTableEntry() {
        if (!root.children.isEmpty() && root.children.get(0) instanceof GroupSymbolTableNode) {
            GroupSymbolTableNode rootNode = (GroupSymbolTableNode) root.children.get(0);
            return rootNode.getEntries().isEmpty() ? null : rootNode.getEntries().get(0);
        }
        return null;
    }
}
