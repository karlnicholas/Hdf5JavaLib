package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.AllocationRecord;
import org.hdf5javalib.redo.AllocationType;
import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents an HDF5 Symbol Table Node (SNOD) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfGroupSymbolTableNode} class models a symbol table node, which stores
 * a list of symbol table entries for objects (such as datasets or groups) within an
 * HDF5 group. It supports reading from a file channel, writing to a buffer, and
 * managing symbol table entries.
 * </p>
 *
 * @see HdfDataFile
 * @see HdfSymbolTableEntry
 */
public class HdfGroupSymbolTableNode extends AllocationRecord {
    /** The signature of the symbol table node ("SNOD"). */
    private final String signature;
    /** The version of the symbol table node format. */
    private final int version;
    /** The list of symbol table entries. */
    private final List<HdfSymbolTableEntry> symbolTableEntries;

    /**
     * Constructs an HdfGroupSymbolTableNode with the specified fields.
     *
     * @param signature          the signature of the node ("SNOD")
     * @param version            the version of the node format
     * @param symbolTableEntries the list of symbol table entries
     */
    public HdfGroupSymbolTableNode(
            String signature,
            int version,
            List<HdfSymbolTableEntry> symbolTableEntries,
            HdfDataFile hdfDataFile,
            String name,
            HdfFixedPoint offset
    ) {
        super(AllocationType.SNOD, name, offset,
                hdfDataFile.getFileAllocation().HDF_SNOD_STORAGE_SIZE
        );
        this.signature = signature;
        this.version = version;
        this.symbolTableEntries = symbolTableEntries;
    }

    /**
     * Reads an HdfGroupSymbolTableNode from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfGroupSymbolTableNode
     * @throws IOException if an I/O error occurs or the SNOD signature is invalid
     * @throws IllegalArgumentException if the SNOD signature is invalid
     */
    public static HdfGroupSymbolTableNode readFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws Exception {
        long offset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();
        String signature;
        int version;
        int numberOfSymbols;

        // Read Signature (4 bytes)
        byte[] signatureBytes = new byte[4];
        buffer.get(signatureBytes);
        signature = new String(signatureBytes, StandardCharsets.US_ASCII);
        if (!"SNOD".equals(signature)) {
            throw new IllegalArgumentException("Invalid SNOD signature: " + signature);
        }

        // Read Version (1 byte)
        version = Byte.toUnsignedInt(buffer.get());

        // Skip Reserved Bytes (1 byte)
        buffer.get();

        // Read Number of Symbols (2 bytes, little-endian)
        numberOfSymbols = Short.toUnsignedInt(buffer.getShort());

        // Read Symbol Table Entries
        List<HdfSymbolTableEntry> symbolTableEntries = new ArrayList<>(numberOfSymbols);
        for (int i = 0; i < numberOfSymbols; i++) {
            HdfSymbolTableEntry entry = HdfSymbolTableEntry.readFromSeekableByteChannel(fileChannel, hdfDataFile);
            symbolTableEntries.add(entry);
        }

        return new HdfGroupSymbolTableNode(
                signature,
                version,
                symbolTableEntries,
                hdfDataFile,
                "SNOD Block " + (hdfDataFile.getFileAllocation().getSnodAllocationCount()),
                HdfWriteUtils.hdfFixedPointFromValue(offset, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength()));
    }

    /**
     * Writes the symbol table node to a ByteBuffer.
     *
     * @param buffer the ByteBuffer to write to
     */
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.put(signature.getBytes(StandardCharsets.US_ASCII));
        buffer.put((byte) version);
        buffer.put((byte) 0);
        buffer.putShort((short) symbolTableEntries.size());
        for (HdfSymbolTableEntry symbolTableEntry : symbolTableEntries) {
            symbolTableEntry.writeToBuffer(buffer);
        }
    }

    /**
     * Returns a string representation of the HdfGroupSymbolTableNode.
     *
     * @return a string describing the node's signature, version, number of symbols, and entries
     */
    @Override
    public String toString() {
        return "HdfGroupSymbolTableNode{" +
                "signature='" + signature + '\'' +
                ", version=" + version +
                ", numberOfSymbols=" + symbolTableEntries.size() +
                ", symbolTableEntries=\r\n" + symbolTableEntries +
                "\r\n}";
    }

    /**
     * Adds a symbol table entry to the node.
     *
     * @param hdfSymbolTableEntry the symbol table entry to add
     */
    public void addEntry(HdfSymbolTableEntry hdfSymbolTableEntry) {
        symbolTableEntries.add(hdfSymbolTableEntry);
    }

    /**
     * Returns the number of symbols in the node.
     *
     * @return the number of symbol table entries
     */
    public int getNumberOfSymbols() {
        return symbolTableEntries.size();
    }

    public List<HdfSymbolTableEntry> getSymbolTableEntries() {
        return symbolTableEntries;
    }
}