package org.hdf5javalib.file.infrastructure;

import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Getter
public class HdfGroupSymbolTableNode {
    private final String signature; // Should be "SNOD"
    private final int version;
    private final List<HdfSymbolTableEntry> symbolTableEntries;

    public HdfGroupSymbolTableNode(String signature, int version, List<HdfSymbolTableEntry> symbolTableEntries) {
        this.signature = signature;
        this.version = version;
        this.symbolTableEntries = symbolTableEntries;
    }

    public static HdfGroupSymbolTableNode readFromFileChannel(SeekableByteChannel fileChannel, short offsetSize) throws IOException {
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

        // Read Symbol Table Entries
        List<HdfSymbolTableEntry> symbolTableEntries = new ArrayList<>(numberOfSymbols);
        for (int i = 0; i < numberOfSymbols; i++) {
            HdfSymbolTableEntry entry = HdfSymbolTableEntry.readFromFileChannel(fileChannel, offsetSize);
            symbolTableEntries.add(entry);
        }

        return new HdfGroupSymbolTableNode(signature, version, symbolTableEntries);
    }

    public void writeToBuffer(ByteBuffer buffer) {
        buffer.put(signature.getBytes(StandardCharsets.US_ASCII));
        buffer.put((byte) version);
        buffer.put((byte) 0);
        buffer.putShort((short) symbolTableEntries.size());
        for (HdfSymbolTableEntry symbolTableEntry : symbolTableEntries) {
            symbolTableEntry.writeToBuffer(buffer);
        }
    }

    @Override
    public String toString() {
        return "HdfGroupSymbolTableNode{" +
                "signature='" + signature + '\'' +
                ", version=" + version +
                ", numberOfSymbols=" + symbolTableEntries.size() +
                ", symbolTableEntries=\r\n" + symbolTableEntries +
                "\r\n}";
    }

    public void addEntry(HdfSymbolTableEntry hdfSymbolTableEntry) {
        symbolTableEntries.add(hdfSymbolTableEntry);
    }

    public int getNumberOfSymbols() {
        return symbolTableEntries.size();
    }
}