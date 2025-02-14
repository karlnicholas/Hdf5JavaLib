package com.github.karlnicholas.hdf5javalib;

public class BTreeTest {
    public static void main(String[] args) {
        SymbolTableEntry rootEntry = new SymbolTableEntry("ROOT", "0");
        BTree btree = new BTree(4, 16, rootEntry);

        // Insert entries
        btree.insert(10, new SymbolTableEntry("A", "10"));
        btree.insert(20, new SymbolTableEntry("B", "20"));
        btree.insert(30, new SymbolTableEntry("C", "30"));
        btree.insert(40, new SymbolTableEntry("D", "40"));
        btree.insert(50, new SymbolTableEntry("E", "50")); // Should trigger split

        // Verify root entry
        SymbolTableEntry actualRoot = btree.getRootSymbolTableEntry();
        if (actualRoot != null) {
            System.out.println("Actual Root Symbol: " + actualRoot.getSymbol() + ", Value: " + actualRoot.getValue());
        } else {
            System.out.println("Actual Root Symbol: NULL");
        }

        if (btree.isRootStillInternal()) {
            System.out.println("Test passed: Root remains an internal node.");
        } else {
            System.out.println("Test failed: Root has been moved to a leaf.");
        }

        if (!btree.isRootEntryModified()) {
            System.out.println("Test passed: Root entry is unchanged.");
        } else {
            System.out.println("Test failed: Root entry was modified.");
        }

        // Print final B-tree structure
        btree.traverse();
    }
}
