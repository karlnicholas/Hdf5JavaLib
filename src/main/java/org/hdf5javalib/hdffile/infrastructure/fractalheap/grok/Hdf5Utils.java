package org.hdf5javalib.hdffile.infrastructure.fractalheap.grok;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;

//##############################################################################
//### HDF5 UTILITY CLASSES
//##############################################################################
class Hdf5Utils {

    public static final long UNDEFINED_ADDRESS = -1L;

    public static void readBytes(SeekableByteChannel channel, ByteBuffer buffer) throws IOException {
        buffer.clear();
        int bytesRead = channel.read(buffer);
        if (bytesRead != buffer.limit()) {
            throw new IOException("Failed to read required number of bytes. Expected "
                    + buffer.limit() + ", got " + bytesRead);
        }
        buffer.flip();
    }

    public static String readSignature(ByteBuffer bb, String expectedSignature) throws IOException {
        byte[] signatureBytes = new byte[4];
        bb.get(signatureBytes);
        String signature = new String(signatureBytes, StandardCharsets.US_ASCII);
        if (expectedSignature != null && !expectedSignature.equals(signature)) {
            throw new IOException("Invalid signature. Expected '" + expectedSignature
                    + "' but found '" + signature + "'");
        }
        return signature;
    }

    public static long readOffset(ByteBuffer bb, int sizeOfOffsets) {
        long value = readVariableSizeUnsigned(bb, sizeOfOffsets);
        long allOnes = (sizeOfOffsets == 8) ? -1L : (1L << (sizeOfOffsets * 8)) - 1;
        return (value == allOnes) ? UNDEFINED_ADDRESS : value;
    }

    public static long readLength(ByteBuffer bb, int sizeOfLengths) {
        return readVariableSizeUnsigned(bb, sizeOfLengths);
    }

    public static long readVariableSizeUnsigned(ByteBuffer bb, int size) {
        if (size > 8 || size < 1) {
            throw new IllegalArgumentException("Variable size must be between 1 and 8 bytes.");
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

    public static int ceilLog2(long value) {
        if (value == 0) return 0;
        return 64 - Long.numberOfLeadingZeros(value - 1);
    }

    /** Helper for reading bit-packed fields */
    static class BitReader {
        private final byte[] data;
        private int bitPosition;

        public BitReader(byte[] data) {
            this.data = data;
            this.bitPosition = 0;
        }

        public long read(int numBits) {
            if (numBits == 0) return 0;
            long value = 0;
            for (int i = 0; i < numBits; i++) {
                int currentBitPos = bitPosition++;
                int byteIndex = currentBitPos / 8;
                int bitIndexInByte = currentBitPos % 8;
                if ((data[byteIndex] & (1 << bitIndexInByte)) != 0) {
                    value |= (1L << i);
                }
            }
            return value;
        }
    }

    /** Helper for writing bit-packed fields (for testing) */
    static class BitWriter {
        private final byte[] data;
        private int bitPosition;

        public BitWriter(int numBytes) {
            this.data = new byte[numBytes];
            this.bitPosition = 0;
        }

        public void write(long value, int numBits) {
            for (int i = 0; i < numBits; i++) {
                if ((value & (1L << i)) != 0) {
                    int currentBitPos = bitPosition;
                    int byteIndex = currentBitPos / 8;
                    int bitIndexInByte = currentBitPos % 8;
                    data[byteIndex] |= (1 << bitIndexInByte);
                }
                bitPosition++;
            }
        }
        public byte[] toArray() { return data; }
    }
}
