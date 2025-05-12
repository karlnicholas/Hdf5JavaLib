package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;

/**
 * Represents an entry in an HDF5 B-Tree (version 1) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfBTreeEntry} class models a single entry in a B-Tree, which is used for indexing
 * group entries in HDF5 files. Each entry contains a key, a child pointer (address), and a payload
 * that is either a symbol table node (for leaf entries) or a child B-Tree (for internal entries).
 * Only one type of payload is non-null at a time.
 * </p>
 *
 * @see HdfFixedPoint
 * @see HdfGroupSymbolTableNode
 * @see HdfBTreeV1
 */
public class HdfBTreeSnodEntry extends HdfBTreeEntry {
// THIS
    /** The symbol table node payload, non-null for leaf entries (nodeLevel == 0). */
    private final HdfGroupSymbolTableNode symbolTableNode;

    /**
     * Constructs an HdfBTreeEntry for a leaf node, pointing to a symbol table node.
     *
     * @param key             the key for the entry
     * @param childPointer     the address of the symbol table node
     * @param symbolTableNode the symbol table node payload
     */
    public HdfBTreeSnodEntry(HdfFixedPoint key, HdfFixedPoint childPointer, HdfGroupSymbolTableNode symbolTableNode) {
        super(key, childPointer);
        this.symbolTableNode = symbolTableNode;
    }

    /**
     * Returns a string representation of the HdfBTreeEntry.
     * <p>
     * Includes the key, child pointer, and the full payload content (symbol table node
     * or child B-Tree) by calling their respective {@code toString()} methods.
     * </p>
     *
     * @return a string representation of the entry
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HdfBTreeEntry{");
        sb.append(super.toString());
        sb.append(", payload(SNOD)=").append(symbolTableNode);
        sb.append('}');
        return sb.toString();
    }

    public HdfGroupSymbolTableNode getSymbolTableNode() {
        return symbolTableNode;
    }

}