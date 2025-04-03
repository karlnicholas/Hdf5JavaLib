package org.hdf5javalib.file.infrastructure;

import lombok.Getter; // Use Getter instead of Data if setters aren't needed/wanted
import org.hdf5javalib.dataclass.HdfFixedPoint;

@Getter // Using Getter only
public class HdfBTreeEntry {
    private final HdfFixedPoint key;
    private final HdfFixedPoint childPointer; // Keep original address for reference/debug

    // Payload: Only one of these should be non-null
    private final HdfGroupSymbolTableNode symbolTableNode; // Populated if parent nodeLevel == 0

    private final HdfBTreeV1 childBTree; // Populated if parent nodeLevel > 0

    // Constructor for Leaf Node Entries (points to SNOD)
    public HdfBTreeEntry(HdfFixedPoint key, HdfFixedPoint snodAddress, HdfGroupSymbolTableNode symbolTableNode) {
        this.key = key;
        this.childPointer = snodAddress;
        this.symbolTableNode = symbolTableNode; // SNOD is the payload
        this.childBTree = null;                 // No child BTree
    }

    // Constructor for Internal Node Entries (points to another BTree)
    public HdfBTreeEntry(HdfFixedPoint key, HdfFixedPoint childBTreeAddress, HdfBTreeV1 childBTree) {
        this.key = key;
        this.childPointer = childBTreeAddress;
        this.symbolTableNode = null;            // No SNOD directly
        this.childBTree = childBTree;           // Child BTree is the payload
    }

    // Optional: Add helper methods
    public boolean isLeafEntry() {
        return this.symbolTableNode != null;
    }

    public boolean isInternalEntry() {
        return this.childBTree != null;
    }
}