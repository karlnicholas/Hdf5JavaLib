package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDisplayUtils;

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
 * @see org.hdf5javalib.hdffile.infrastructure.HdfGroupForGroupBTreeEntry
 */
public abstract class HdfBTreeV1 {
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
     * Constructs an HdfTree with all fields specified.
     *
     * @param nodeType            the type of the node (0 for group B-Tree)
     * @param nodeLevel           the level of the node (0 for leaf, >0 for internal)
     * @param entriesUsed         the number of entries used in the node
     * @param leftSiblingAddress  the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     */
    public HdfBTreeV1(
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress
    ) {
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
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
        sb.append(", nodeType=").append(nodeType);
        sb.append(", nodeLevel=").append(nodeLevel);
        sb.append(", entriesUsed=").append(entriesUsed);
        sb.append(", leftSiblingAddress=").append(leftSiblingAddress.isUndefined() ?HdfDisplayUtils.UNDEFINED: leftSiblingAddress);
        sb.append(", rightSiblingAddress=").append(rightSiblingAddress.isUndefined() ?HdfDisplayUtils.UNDEFINED: rightSiblingAddress);
        sb.append(", entries=[");
        sb.append("]");
        sb.append('}');
        return sb.toString();
    }

    public int getNodeLevel() {
        return nodeLevel;
    }
}