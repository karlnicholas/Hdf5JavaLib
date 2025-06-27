package org.hdf5javalib.maydo.hdffile.infrastructure;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdfjava.AllocationRecord;
import org.hdf5javalib.maydo.hdfjava.AllocationType;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
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
public class HdfGroupSymbolTableNode {
    private static final byte[] GROUP_SYMBOL_TABLE_NODE_SIGNATURE = {'S', 'N', 'O', 'D'};
    /**
     * The version of the symbol table node format.
     */
    private final int version;
    /**
     * The list of symbol table entries.
     */
    private final List<HdfSymbolTableEntry> symbolTableEntries;

    /**
     * Constructs an HdfGroupSymbolTableNode with the specified fields.
     *
     * @param version            the version of the node format
     * @param symbolTableEntries the list of symbol table entries
     */
    public HdfGroupSymbolTableNode(
            int version,
            List<HdfSymbolTableEntry> symbolTableEntries,
            HdfDataFile hdfDataFile,
            String name,
            HdfFixedPoint offset
    ) {
        this.version = version;
        this.symbolTableEntries = symbolTableEntries;
    }

    /**
     * Reads an HdfGroupSymbolTableNode from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfGroupSymbolTableNode
     * @throws IOException              if an I/O error occurs or the SNOD signature is invalid
     * @throws IllegalArgumentException if the SNOD signature is invalid
     */
    public static HdfGroupSymbolTableNode readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            HdfLocalHeap localHeap,
            String objectName
    ) throws Exception {
        long offset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();
        int version;
        int numberOfSymbols;

        // Read Signature (4 bytes)
        byte[] signatureBytes = new byte[GROUP_SYMBOL_TABLE_NODE_SIGNATURE.length];
        buffer.get(signatureBytes);
        if (Arrays.compare(GROUP_SYMBOL_TABLE_NODE_SIGNATURE, signatureBytes) != 0) {
            throw new IllegalArgumentException("Invalid SNOD signature: " + Arrays.toString(signatureBytes));
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
            HdfSymbolTableEntry entry = HdfSymbolTableEntry.readFromSeekableByteChannel(fileChannel, hdfDataFile, localHeap);
            symbolTableEntries.add(entry);
        }

        return new HdfGroupSymbolTableNode(
                version,
                symbolTableEntries,
                hdfDataFile,
                objectName + ":Snod",
                HdfWriteUtils.hdfFixedPointFromValue(offset, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength()));
    }

    /**
     * Writes the symbol table node to a ByteBuffer.
     *
     * @param buffer the ByteBuffer to write to
     */
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.put(GROUP_SYMBOL_TABLE_NODE_SIGNATURE);
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
                "signature='" + new String(GROUP_SYMBOL_TABLE_NODE_SIGNATURE) + '\'' +
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