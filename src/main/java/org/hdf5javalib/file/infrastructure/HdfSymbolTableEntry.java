package org.hdf5javalib.file.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.BitSet;

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
 * @see org.hdf5javalib.dataclass.HdfFixedPoint
 * @see org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype
 */
public class HdfSymbolTableEntry {
    /** The offset of the link name in the local heap. */
    private final HdfFixedPoint linkNameOffset;
    /** The offset of the object's header in the file. */
    private final HdfFixedPoint objectHeaderOffset;
    /** The cache type (0 for basic, 1 for additional B-Tree and heap offsets). */
    private final int cacheType;
    /** The offset of the B-Tree for cache type 1 entries (null for cache type 0). */
    private final HdfFixedPoint bTreeOffset;
    /** The offset of the local heap for cache type 1 entries (null for cache type 0). */
    private final HdfFixedPoint localHeapOffset;

    /**
     * Constructs an HdfSymbolTableEntry for cache type 1 with B-Tree and local heap offsets.
     *
     * @param linkNameOffset      the offset of the link name in the local heap
     * @param objectHeaderOffset  the offset of the object's header in the file
     * @param bTreeOffset         the offset of the B-Tree
     * @param localHeapOffset     the offset of the local heap
     */
    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderOffset, HdfFixedPoint bTreeOffset, HdfFixedPoint localHeapOffset) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderOffset = objectHeaderOffset;
        this.cacheType = 1;
        this.bTreeOffset = bTreeOffset;
        this.localHeapOffset = localHeapOffset;
    }

    /**
     * Constructs an HdfSymbolTableEntry for cache type 0 with basic fields only.
     *
     * @param linkNameOffset      the offset of the link name in the local heap
     * @param objectHeaderOffset  the offset of the object's header in the file
     */
    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderOffset) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderOffset = objectHeaderOffset;
        this.cacheType = 0;
        this.bTreeOffset = null;
        this.localHeapOffset = null;
    }

    /**
     * Reads an HdfSymbolTableEntry from a file channel.
     *
     * @param fileChannel                the file channel to read from
     * @param fixedPointDatatypeForOffset the datatype for offset fields
     * @return the constructed HdfSymbolTableEntry
     * @throws IOException if an I/O error occurs
     */
    public static HdfSymbolTableEntry readFromFileChannel(
            SeekableByteChannel fileChannel,
            FixedPointDatatype fixedPointDatatypeForOffset
    ) throws IOException {
        BitSet emptyBitSet = new BitSet();
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);
        HdfFixedPoint objectHeaderAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);

        // Read cache type and skip reserved field
        int cacheType = HdfReadUtils.readIntFromFileChannel(fileChannel);
        HdfReadUtils.skipBytes(fileChannel, 4); // Skip reserved field

        // Initialize addresses for cacheType 1
        if (cacheType == 1) {
            HdfFixedPoint bTreeAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);
            HdfFixedPoint localHeapAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, bTreeAddress, localHeapAddress);
        } else {
            HdfReadUtils.skipBytes(fileChannel, 16); // Skip 16 bytes for scratch-pad
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress);
        }
    }

    /**
     * Writes the symbol table entry to a ByteBuffer.
     *
     * @param buffer the ByteBuffer to write to
     */
    public void writeToBuffer(ByteBuffer buffer) {
        // Write Link Name Offset (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, linkNameOffset);

        // Write Object Header Address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, objectHeaderOffset);

        // Write Cache Type (4 bytes, little-endian)
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        // If cacheType == 1, write B-tree Address and Local Heap Address
        if (cacheType == 1) {
            writeFixedPointToBuffer(buffer, bTreeOffset);
            writeFixedPointToBuffer(buffer, localHeapOffset);
        } else {
            // If cacheType != 1, write 16 bytes of reserved "scratch-pad" space
            buffer.put(new byte[16]);
        }
    }

    /**
     * Returns a string representation of the HdfSymbolTableEntry.
     *
     * @return a string describing the entry's fields based on its cache type
     * @throws IllegalStateException if the cache type is unknown
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HdfSymbolTableEntry{");
        sb.append("linkNameOffset=").append(linkNameOffset)
                .append(", objectHeaderOffset=").append(objectHeaderOffset)
                .append(", cacheType=").append(cacheType);

        switch (cacheType) {
            case 0:
                break; // Base fields only
            case 1:
                sb.append(", bTreeOffset=").append(bTreeOffset)
                        .append(", localHeapOffset=").append(localHeapOffset);
                break;
            default:
                throw new IllegalStateException("Unknown cache type: " + cacheType);
        }

        sb.append("}");
        return sb.toString();
    }

    public HdfFixedPoint getLinkNameOffset() {
        return linkNameOffset;
    }

    public HdfFixedPoint getObjectHeaderOffset() {
        return objectHeaderOffset;
    }

    public HdfFixedPoint getLocalHeapOffset() {
        return localHeapOffset;
    }

    public HdfFixedPoint getBTreeOffset() {
        return bTreeOffset;
    }
}