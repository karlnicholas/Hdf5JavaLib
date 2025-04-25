package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.Setter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

@Getter // Using Getter only
public class HdfBTreeEntry {
    @Setter
    private HdfFixedPoint key;
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

    // Modified toString() to print the full payload content
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HdfBTreeEntry{");
        sb.append("key=").append(key);
        sb.append(", childPointer=").append(childPointer);

        // Append the payload by calling its toString() method
        if (isLeafEntry()) {
            sb.append(", payload(SNOD)=").append(symbolTableNode); // Calls symbolTableNode.toString()
        } else if (isInternalEntry()) {
            sb.append(", payload(ChildBTree)=").append(childBTree); // Calls childBTree.toString()
        } else {
            // This case shouldn't happen with the current constructors
            sb.append(", payload=<empty/unknown>");
        }

        sb.append('}');
        return sb.toString();
    }
}