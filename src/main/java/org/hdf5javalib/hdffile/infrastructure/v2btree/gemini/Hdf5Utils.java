package org.hdf5javalib.hdffile.infrastructure.v2btree.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Hdf5Utils {

    public static final long UNDEFINED_ADDRESS = -1L;

    public static void readBytes(SeekableByteChannel channel, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int bytesRead = channel.read(buffer);
        if (bytesRead != buffer.limit()) {
            throw new IOException("Failed to read the required number of bytes. Expected "
                    + buffer.limit() + ", got " + bytesRead);
        }
        buffer.flip();
    }

    public static String readSignature(ByteBuffer bb) throws IOException {
        byte[] signatureBytes = new byte[4];
        bb.get(signatureBytes);
        return new String(signatureBytes, StandardCharsets.US_ASCII);
    }

    public static void V2_verifySignature(ByteBuffer bb, String expectedSignature) throws IOException {
        String signature = readSignature(bb);
        if (!expectedSignature.equals(signature)) {
            throw new IOException("Invalid B-tree v2 signature. Expected '" + expectedSignature
                    + "' but found '" + signature + "'");
        }
    }

    public static long readOffset(ByteBuffer bb, int sizeOfOffsets) {
        long value = readVariableSizeUnsigned(bb, sizeOfOffsets);
        // In HDF5, all 1s represents an undefined address.
        long allOnes = (sizeOfOffsets == 8) ? -1L : (1L << (sizeOfOffsets * 8)) - 1;
        return (value == allOnes) ? UNDEFINED_ADDRESS : value;
    }

    public static long readLength(ByteBuffer bb, int sizeOfLengths) {
        return readVariableSizeUnsigned(bb, sizeOfLengths);
    }

    public static long readVariableSizeUnsigned(ByteBuffer bb, int size) {
        if (size > 8 || size < 1) {
            throw new IllegalArgumentException("Variable size must be between 1 and 8 bytes. Got: " + size);
        }
        long value = 0;
        for (int i = 0; i < size; i++) {
            value |= ((long) (bb.get() & 0xFF)) << (i * 8);
        }
        return value;
    }

    public static int readChecksum(ByteBuffer bb) {
        return bb.getInt();
    }

    /**
     * Calculates the minimum number of bytes needed to store a given value.
     */
    public static int bytesNeededFor(long value) {
        if (value == 0) return 1;
        int bits = 64 - Long.numberOfLeadingZeros(value);
        return (bits + 7) / 8;
    }
}