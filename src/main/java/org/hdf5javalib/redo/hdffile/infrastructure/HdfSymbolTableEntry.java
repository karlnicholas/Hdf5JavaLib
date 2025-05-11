package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.datatype.FixedPointDatatype;
import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

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
    /** The offset of the link name in the local heap. */
    private final HdfFixedPoint linkNameOffset;
    /** The offset of the object's header in the file. */
    private final HdfFixedPoint objectHeaderOffset;
    private final HdfSymbolTableEntryCache cache;

    /**
     * Constructs an HdfSymbolTableEntry for cache type 1 with B-Tree and local heap offsets.
     *
     * @param linkNameOffset      the offset of the link name in the local heap
     * @param objectHeaderOffset  the offset of the object's header in the file
     * @param cache           the chache type instance for this Symbol Table Entry.
     */
    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderOffset, HdfSymbolTableEntryCache cache) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderOffset = objectHeaderOffset;
        this.cache = cache;
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
        this.cache = new HdfSymbolTableEntryCacheNotUsed();
    }

    /**
     * Reads an HdfSymbolTableEntry from a file channel.
     *
     * @param fileChannel                the file channel to read from
     * @param hdfDataFile the HdfDataFile offset fields
     * @return the constructed HdfSymbolTableEntry
     * @throws IOException if an I/O error occurs
     */
    public static HdfSymbolTableEntry readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile
    ) throws Exception {
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        HdfFixedPoint objectHeaderAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);

        // Read cache type and skip reserved field
        int cacheType = HdfReadUtils.readIntFromFileChannel(fileChannel);
        HdfReadUtils.skipBytes(fileChannel, 4); // Skip reserved field

        // Initialize addresses for cacheType 1
        if (cacheType == 1) {
            HdfSymbolTableEntryCache cache = HdfSymbolTableEntryCacheGroupMetadata.readFromSeekableByteChannel(fileChannel, hdfDataFile);
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, cache);
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
        cache.writeToBuffer(buffer);
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
                .append(", cacheType=").append(cache);

        switch (cache.getCacheType()) {
            case 0:
                break; // Base fields only
            case 1:
                sb.append(", bTreeOffset=").append(bTreeOffset)
                        .append(", localHeapOffset=").append(localHeapOffset);
                break;
            default:
                throw new IllegalStateException("Unknown cache type: " + cache);
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