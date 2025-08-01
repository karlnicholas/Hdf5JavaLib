package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;

/**
 * Represents an entry in an HDF5 B-Tree (version 1) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfGroupBTreeEntry} class models a single entry in a B-Tree, which is used for indexing
 * group entries in HDF5 files. Each entry contains a key, a child pointer (address), and a payload
 * that is either a symbol table node (for leaf entries) or a child B-Tree (for internal entries).
 * Only one type of payload is non-null at a time.
 * </p>
 *
 * @see HdfFixedPoint
 * @see HdfGroupSymbolTableNode
 * @see HdfBTreeV1
 */
public abstract class HdfBTreeEntryBase {
    /**
     * The key for this B-Tree entry, linkNameOffset.
     */
    private HdfFixedPoint key;
    /**
     * The address of the child node (symbol table node or child B-Tree).
     */
    private final HdfFixedPoint childPointer;
    /**
     * The child B-Tree payload, non-null for internal entries (nodeLevel > 0).
     */
    private final HdfBTreeV1 childBTree;


    /**
     * Constructs an HdfGroupBTreeEntry for a leaf node, pointing to a symbol table node.
     *
     * @param key          the key for the entry
     * @param childPointer the address of the symbol table node
     */
    public HdfBTreeEntryBase(HdfFixedPoint key, HdfFixedPoint childPointer, HdfBTreeV1 childBTree) {
        this.key = key;
        this.childPointer = childPointer;
        this.childBTree = childBTree;
    }

    public HdfBTreeV1 getChildBTree() {
        return childBTree;
    }

    //    /**
//     * Constructs an HdfGroupBTreeEntry for an internal node, pointing to a child B-Tree.
//     *
//     * @param key              the key for the entry
//     * @param childBTreeAddress the address of the child B-Tree
//     * @param childBTree       the child B-Tree payload
//     */
//    public HdfGroupBTreeEntry(HdfFixedPoint key, HdfFixedPoint childBTreeAddress, HdfBTree childBTree) {
//        this.key = key;
//        this.childPointer = childBTreeAddress;
//        this.symbolTableNode = null;
//        this.childBTree = childBTree;
//    }
//
//    /**
//     * Checks if this is a leaf entry (contains a symbol table node).
//     *
//     * @return true if the entry is a leaf entry, false otherwise
//     */
//    public boolean isLeafEntry() {
//        return this.symbolTableNode != null;
//    }
//
//    /**
//     * Checks if this is an internal entry (contains a child B-Tree).
//     *
//     * @return true if the entry is an internal entry, false otherwise
//     */
//    public boolean isInternalEntry() {
//        return this.childBTree != null;
//    }

    /**
     * Returns a string representation of the HdfGroupBTreeEntry.
     * <p>
     * Includes the key, child pointer, and the full payload content (symbol table node
     * or child B-Tree) by calling their respective {@code toString()} methods.
     * </p>
     *
     * @return a string representation of the entry
     */
    @Override
    public String toString() {
        return "key="+key+", childPointer=\"" + childPointer + "\"";
    }

    public void setKey(HdfFixedPoint key) {
        this.key = key;
    }

    public HdfFixedPoint getKey() {
        return key;
    }

    public HdfFixedPoint getChildPointer() {
        return childPointer;
    }

}