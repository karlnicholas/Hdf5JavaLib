package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

/**
 * Represents an entry in an HDF5 B-Tree (version 1) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfBTreeEntry} class models a single entry in a B-Tree, which is used for indexing
 * group entries in HDF5 files. Each entry contains a key, a child pointer (address), and a payload
 * that is either a symbol table node (for leaf entries) or a child B-Tree (for internal entries).
 * Only one type of payload is non-null at a time.
 * </p>
 *
 * @see org.hdf5javalib.dataclass.HdfFixedPoint
 * @see org.hdf5javalib.file.infrastructure.HdfGroupSymbolTableNode
 * @see org.hdf5javalib.file.infrastructure.HdfBTreeV1
 */
@Getter
public class HdfBTreeEntry {
    /** The key for this B-Tree entry, used for ordering. */
    @Setter
    private HdfFixedPoint key;
    /** The address of the child node (symbol table node or child B-Tree). */
    private final HdfFixedPoint childPointer;
    /** The symbol table node payload, non-null for leaf entries (nodeLevel == 0). */
    private final HdfGroupSymbolTableNode symbolTableNode;
    /** The child B-Tree payload, non-null for internal entries (nodeLevel > 0). */
    private final HdfBTreeV1 childBTree;

    /**
     * Constructs an HdfBTreeEntry for a leaf node, pointing to a symbol table node.
     *
     * @param key             the key for the entry
     * @param snodAddress     the address of the symbol table node
     * @param symbolTableNode the symbol table node payload
     */
    public HdfBTreeEntry(HdfFixedPoint key, HdfFixedPoint snodAddress, HdfGroupSymbolTableNode symbolTableNode) {
        this.key = key;
        this.childPointer = snodAddress;
        this.symbolTableNode = symbolTableNode;
        this.childBTree = null;
    }

    /**
     * Constructs an HdfBTreeEntry for an internal node, pointing to a child B-Tree.
     *
     * @param key              the key for the entry
     * @param childBTreeAddress the address of the child B-Tree
     * @param childBTree       the child B-Tree payload
     */
    public HdfBTreeEntry(HdfFixedPoint key, HdfFixedPoint childBTreeAddress, HdfBTreeV1 childBTree) {
        this.key = key;
        this.childPointer = childBTreeAddress;
        this.symbolTableNode = null;
        this.childBTree = childBTree;
    }

    /**
     * Checks if this is a leaf entry (contains a symbol table node).
     *
     * @return true if the entry is a leaf entry, false otherwise
     */
    public boolean isLeafEntry() {
        return this.symbolTableNode != null;
    }

    /**
     * Checks if this is an internal entry (contains a child B-Tree).
     *
     * @return true if the entry is an internal entry, false otherwise
     */
    public boolean isInternalEntry() {
        return this.childBTree != null;
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
        sb.append("key=").append(key);
        sb.append(", childPointer=").append(childPointer);

        // Append the payload by calling its toString() method
        if (isLeafEntry()) {
            sb.append(", payload(SNOD)=").append(symbolTableNode);
        } else if (isInternalEntry()) {
            sb.append(", payload(ChildBTree)=").append(childBTree);
        } else {
            sb.append(", payload=<empty/unknown>");
        }

        sb.append('}');
        return sb.toString();
    }
}