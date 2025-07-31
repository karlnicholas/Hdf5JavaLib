package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datatype.FixedPointDatatype;

import java.nio.ByteBuffer;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents an HDF5 Symbol Table Entry as defined in the HDF5 specification.
 * <p>
 * The {@code HdfSymbolTableEntry} class models a single entry in a symbol table,
 * which contains metadata about an object (e.g., dataset or group) within an HDF5
 * group. It includes the link name offset, object header offset, and optional
 * fields for cache type 1 entries (B-Tree and local heap offsets). This class
 * supports reading from a file channel and writing to a buffer.
 * </p>
 *
 * @see HdfFixedPoint
 * @see FixedPointDatatype
 */
public class HdfSymbolTableEntry {
    public final static int RESERVED_FIELD_1_SIZE = 4;
    /**
     * The offset of the link name in the local heap.
     */
    private final HdfFixedPoint linkNameOffset;
    /**
     * The offset of the object's header in the file.
     */
    private final HdfFixedPoint objectHeaderAddress;
    private final HdfSymbolTableEntryCache cache;

    /**
     * Constructs an HdfSymbolTableEntry for cache type 1 with B-Tree and local heap offsets.
     *
     * @param linkNameOffset the offset of the link name in the local heap
     * @param cache          the cache type instance for this Symbol Table Entry.
     */
    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderAddress, HdfSymbolTableEntryCache cache) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderAddress = objectHeaderAddress;
        this.cache = cache;
    }

    /**
     * Writes the symbol table entry to a ByteBuffer.
     *
     * @param buffer the ByteBuffer to write to
     */
    public void writeToBuffer(ByteBuffer buffer) {
        // Write Link Name Offset (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, linkNameOffset);

    }

    /**
     * Returns a string representation of the HdfSymbolTableEntry.
     *
     * @return a string describing the entry's fields based on its cache type
     * @throws IllegalStateException if the cache type is unknown
     */
    @Override
    public String toString() {
        return "HdfSymbolTableEntry{" + "linkNameOffset=" + linkNameOffset +
                ", cacheType=" + cache +
                "}";
    }

    public HdfFixedPoint getLinkNameOffset() {
        return linkNameOffset;
    }

    public HdfFixedPoint getObjectHeaderAddress() {
        return objectHeaderAddress;
    }

    public HdfSymbolTableEntryCache getCache() {
        return cache;
    }

}