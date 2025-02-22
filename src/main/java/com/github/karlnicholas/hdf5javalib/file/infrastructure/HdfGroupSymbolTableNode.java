package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Getter
public class HdfGroupSymbolTableNode {
    private final String signature; // Should be "SNOD"
    private final int version;
    private int numberOfSymbols;
    private final List<HdfSymbolTableEntry> symbolTableEntries;

    public HdfGroupSymbolTableNode(String signature, int version, int numberOfSymbols, List<HdfSymbolTableEntry> symbolTableEntries) {
        this.signature = signature;
        this.version = version;
        this.numberOfSymbols = numberOfSymbols;
        this.symbolTableEntries = symbolTableEntries;
    }

    public static HdfGroupSymbolTableNode readFromFileChannel(FileChannel fileChannel, short offsetSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();
        String signature; // Should be "SNOD"
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
//        int allocationSize = numberOfSymbols * (16+16+8);
//        fileChannel.read(buffer);
        // Read Symbol Table Entries
//        List symbolTableEntries = new ArrayList<>();
//        for (int i = 0; i < numberOfSymbols; i++) {
//            symbolTableEntries = symbolTableEntries;
//        }
        return new HdfGroupSymbolTableNode(signature, version, numberOfSymbols, new ArrayList<>());
    }

    public void writeToBuffer(ByteBuffer buffer) {
        buffer.put(signature.getBytes(StandardCharsets.US_ASCII));
        buffer.putInt(version);
        buffer.put((byte) 0);
        buffer.putInt(numberOfSymbols);
        buffer.position(0);
        for (HdfSymbolTableEntry symbolTableEntry: symbolTableEntries) {
            symbolTableEntry.writeToByteBuffer(buffer);
        }
    }

    @Override
    public String toString() {
        return "HdfGroupSymbolTableNode{" +
                "signature='" + signature + '\'' +
                ", version=" + version +
                ", numberOfSymbols=" + numberOfSymbols +
                ", symbolTableEntries=\r\n\t\t" + symbolTableEntries +
                "\r\n}";
    }

    public void addEntry(HdfSymbolTableEntry hdfSymbolTableEntry) {
        numberOfSymbols++;
        symbolTableEntries.add(hdfSymbolTableEntry);
    }
}
