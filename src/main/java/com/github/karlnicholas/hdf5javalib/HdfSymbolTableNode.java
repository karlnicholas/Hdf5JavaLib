package com.github.karlnicholas.hdf5javalib;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HdfSymbolTableNode {
    private final String signature; // Should be "SNOD"
    private final int version;
    private final int numberOfSymbols;
    private final List<HdfSymbolTableEntry> symbolTableEntries;

    public HdfSymbolTableNode(ByteBuffer buffer, int offsetSize) {
        // Read Signature (4 bytes)
        byte[] signatureBytes = new byte[4];
        buffer.get(signatureBytes);
        this.signature = new String(signatureBytes, StandardCharsets.US_ASCII);
        if (!"SNOD".equals(signature)) {
            throw new IllegalArgumentException("Invalid SNOD signature: " + signature);
        }

        // Read Version (1 byte)
        this.version = Byte.toUnsignedInt(buffer.get());

        // Skip Reserved Bytes (1 byte)
        buffer.get();

        // Read Number of Symbols (2 bytes, little-endian)
        this.numberOfSymbols = Short.toUnsignedInt(buffer.getShort());

        // Read Symbol Table Entries
        this.symbolTableEntries = new ArrayList<>();
        for (int i = 0; i < numberOfSymbols; i++) {
//            this.symbolTableEntries.add(HdfSymbolTableEntry.fromByteBuffer(buffer, offsetSize));
        }
    }

    public String getSignature() {
        return signature;
    }

    public int getVersion() {
        return version;
    }

    public int getNumberOfSymbols() {
        return numberOfSymbols;
    }

    public List<HdfSymbolTableEntry> getSymbolTableEntries() {
        return symbolTableEntries;
    }

    @Override
    public String toString() {
        return "HdfSymbolTableNode{" +
                "signature='" + signature + '\'' +
                ", version=" + version +
                ", numberOfSymbols=" + numberOfSymbols +
                ", symbolTableEntries=" + symbolTableEntries +
                '}';
    }
}
