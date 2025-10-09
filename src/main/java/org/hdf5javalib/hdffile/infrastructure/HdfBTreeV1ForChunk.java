package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an HDF5 B-Tree (version 1) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfTree} class models a B-Tree used for indexing group entries in HDF5 files.
 * It supports both leaf nodes (containing symbol table nodes) and internal nodes (containing
 * child B-Trees). This class provides methods for reading from a file channel, adding datasets,
 * splitting symbol table nodes, and writing the B-Tree structure back to a file.
 * </p>
 *
 * @see HdfDataFile
 * @see HdfFixedPoint
 * @see HdfGroupForGroupBTreeEntry
 */
public class HdfBTreeV1ForChunk extends HdfBTreeV1{
    /**
     * The list of B-Tree entries.
     */
    private final List<HdfGroupForChunkBTreeEntry> entries;

    /**
     * Constructs an HdfTree with all fields specified.
     *
     * @param nodeType            the type of the node (0 for group B-Tree)
     * @param nodeLevel           the level of the node (0 for leaf, >0 for internal)
     * @param entriesUsed         the number of entries used in the node
     * @param leftSiblingAddress  the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     * @param entries             the list of B-Tree entries
     */
    public HdfBTreeV1ForChunk(
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            List<HdfGroupForChunkBTreeEntry> entries
    ) {
        super(nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress);
        this.entries = entries;
    }

    /**
     * Returns a string representation of the HdfTree.
     *
     * @return a string describing the node's signature, type, level, entries, and structure
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfTree{");
        sb.append("signature='").append("TREE").append('\'');
        sb.append(", entries=[");
        if (entries != null && !entries.isEmpty()) {
            boolean first = true;
            for (HdfGroupForChunkBTreeEntry entry : entries) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(entry);
                first = false;
            }
        } else if (entries != null) {
            sb.append("<empty>");
        } else {
            sb.append("<null>");
        }
        sb.append("]");
        sb.append('}');
        return sb.toString();
    }

    public List<HdfGroupForChunkBTreeEntry> getEntries() {
        return entries;
    }

}