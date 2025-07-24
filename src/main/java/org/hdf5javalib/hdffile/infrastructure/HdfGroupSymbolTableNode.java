package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;

import java.nio.ByteBuffer;
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
    public static final byte[] GROUP_SYMBOL_TABLE_NODE_SIGNATURE = {'S', 'N', 'O', 'D'};
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
            HdfFixedPoint offset
    ) {
        this.version = version;
        this.symbolTableEntries = symbolTableEntries;
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