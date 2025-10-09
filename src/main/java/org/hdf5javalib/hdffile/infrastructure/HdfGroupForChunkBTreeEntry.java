package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.util.List;

/**
 * Represents an entry in an HDF5 B-Tree (version 1) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfGroupForGroupBTreeEntry} class models a single entry in a B-Tree, which is used for indexing
 * group entries in HDF5 files. Each entry contains a key, a child pointer (address), and a payload
 * that is either a symbol table node (for leaf entries) or a child B-Tree (for internal entries).
 * Only one type of payload is non-null at a time.
 * </p>
 *
 * @see HdfFixedPoint
 * @see HdfGroupSymbolTableNode
 * @see HdfBTreeV1
 */
public class HdfGroupForChunkBTreeEntry extends HdfGroupBTreeEntry {
    private final long sizeOfChunk;
    private final long filterMask;
    /**
     * The symbol table node payload, non-null for leaf entries (nodeLevel == 0).
     */
    private final List<HdfFixedPoint> dimensionOffsets;

    public HdfGroupForChunkBTreeEntry(HdfFixedPoint childPointer, HdfBTreeV1ForChunk childBTree, long sizeOfChunk, long filterMask, List<HdfFixedPoint> dimensionOffsets) {
        super(childPointer, childBTree);
        this.sizeOfChunk = sizeOfChunk;
        this.filterMask = filterMask;
        this.dimensionOffsets = dimensionOffsets;
    }

    /**
     * Returns a string representation of the HdfGroupForGroupBTreeEntry.
     * <p>
     * Includes the key, child pointer, and the full payload content (symbol table node
     * or child B-Tree) by calling their respective {@code toString()} methods.
     * </p>
     *
     * @return a string representation of the entry
     */
    @Override
    public String toString() {
        return "HdfChunkBTreeEntry{" +
                super.toString() +
                ", sizeOfChunk=" + sizeOfChunk +
                ", filterMask=" + filterMask +
                ", dimensionOffsets=" + dimensionOffsets +
                ", childPointer=\"" + childPointer + "\"" +
                '}';
    }

    public List<HdfFixedPoint>  getDimensionOffsets() {
        return dimensionOffsets;
    }

    public long getSizeOfChunk() {
        return sizeOfChunk;
    }

    public long getFilterMask() {
        return filterMask;
    }

    public  HdfBTreeV1ForChunk getChildBTree() {
        return (HdfBTreeV1ForChunk)childBTree;
    }

}