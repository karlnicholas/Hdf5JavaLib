package org.hdf5javalib.maydo.hdffile.infrastructure;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfWriteUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an HDF5 B-Tree (version 1) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfBTree} class models a B-Tree used for indexing group entries in HDF5 files.
 * It supports both leaf nodes (containing symbol table nodes) and internal nodes (containing
 * child B-Trees). This class provides methods for reading from a file channel, adding datasets,
 * splitting symbol table nodes, and writing the B-Tree structure back to a file.
 * </p>
 *
 * @see HdfDataFile
 * @see HdfFixedPoint
 * @see org.hdf5javalib.redo.hdffile.infrastructure.HdfBTreeEntry
 */
public class HdfBTreeV1 {
    /**
     * The type of the node (0 for group B-Tree).
     */
    private final int nodeType;
    /**
     * The level of the node (0 for leaf, >0 for internal).
     */
    private final int nodeLevel;
    /**
     * The number of entries used in the node.
     */
    private int entriesUsed;
    /**
     * The address of the left sibling node.
     */
    private final HdfFixedPoint leftSiblingAddress;
    /**
     * The address of the right sibling node.
     */
    private final HdfFixedPoint rightSiblingAddress;
    /**
     * The first key (key zero) of the node.
     */
    private final HdfFixedPoint keyZero;
    /**
     * The list of B-Tree entries.
     */
    private final List<HdfBTreeEntryBase> entries;
    /**
     * The HDF5 file context.
     */
    private final HdfDataFile hdfDataFile;

    /**
     * Constructs an HdfBTree with all fields specified.
     *
     * @param nodeType            the type of the node (0 for group B-Tree)
     * @param nodeLevel           the level of the node (0 for leaf, >0 for internal)
     * @param entriesUsed         the number of entries used in the node
     * @param leftSiblingAddress  the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     * @param keyZero             the first key of the node
     * @param entries             the list of B-Tree entries
     * @param hdfDataFile         the HDF5 file context
     */
    public HdfBTreeV1(
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfFixedPoint keyZero,
            List<HdfBTreeEntryBase> entries,
            HdfDataFile hdfDataFile,
            HdfFixedPoint offset
    ) {
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = keyZero;
        this.entries = entries;
        this.hdfDataFile = hdfDataFile;
    }

    /**
     * Constructs an HdfBTree with minimal fields for a new node.
     *
     * @param nodeType            the type of the node (0 for group B-Tree)
     * @param nodeLevel           the level of the node (0 for leaf, >0 for internal)
     * @param leftSiblingAddress  the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     * @param hdfDataFile         the HDF5 file context
     */
    public HdfBTreeV1(
            int nodeType,
            int nodeLevel,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfDataFile hdfDataFile,
            HdfFixedPoint offset
    ) {
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.hdfDataFile = hdfDataFile;
        this.entriesUsed = 0;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength());
        this.entries = new ArrayList<>();
    }

    /**
     * Returns a string representation of the HdfBTree.
     *
     * @return a string describing the node's signature, type, level, entries, and structure
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfBTree{");
        sb.append("signature='").append(new String("TREE")).append('\'');
        sb.append(", nodeType=").append(nodeType);
        sb.append(", nodeLevel=").append(nodeLevel);
        sb.append(", entriesUsed=").append(entriesUsed);
        sb.append(", leftSiblingAddress=").append(leftSiblingAddress.isUndefined() ? "undefined" : leftSiblingAddress);
        sb.append(", rightSiblingAddress=").append(rightSiblingAddress.isUndefined() ? "undefined" : rightSiblingAddress);
        sb.append(", keyZero=").append(keyZero);
        sb.append(", entries=[");
        if (entries != null && !entries.isEmpty()) {
            boolean first = true;
            for (HdfBTreeEntryBase entry : entries) {
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

    public List<HdfBTreeEntryBase> getEntries() {
        return entries;
    }

}