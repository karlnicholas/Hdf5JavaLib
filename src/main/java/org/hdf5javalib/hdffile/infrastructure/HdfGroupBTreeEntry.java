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
public class HdfGroupBTreeEntry extends HdfBTreeEntryBase {

    /**
     * The symbol table node payload, non-null for leaf entries (nodeLevel == 0).
     */
    private final HdfGroupSymbolTableNode groupSymbolTableNode;

    /**
     * Constructs an HdfGroupBTreeEntry for a leaf node, pointing to a symbol table node.
     *
     * @param key          the key for the entry
     * @param childPointer the address of the symbol table node
     */
    public HdfGroupBTreeEntry(HdfFixedPoint key, HdfFixedPoint childPointer, HdfBTreeV1 childBTree, HdfGroupSymbolTableNode groupSymbolTableNode) {
        super(key, childPointer, childBTree);
        this.groupSymbolTableNode = groupSymbolTableNode;
    }

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
        return super.toString();
    }

    public HdfGroupSymbolTableNode getGroupSymbolTableNode() {
        return groupSymbolTableNode;
    }

//    public HdfGroupSymbolTableNode getSymbolTableNode() {
//        return symbolTableNode;
//    }
//
//    public HdfBTree getChildBTree() {
//        return childBTree;
//    }
}