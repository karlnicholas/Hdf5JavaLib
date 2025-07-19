package org.hdf5javalib.maydo.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ScaleOffsetByteBufferDeflater implements ByteBufferDeflater {
    private final int scaleType;
    private final int scaleFactor;
    private final int datatypeClass;
    private final int datatypeSize;
    private final int signed;
    private final int byteOrder;  // 1 for little-endian

    public ScaleOffsetByteBufferDeflater(int[] clientData) {
        this.scaleType = clientData[0];
        this.scaleFactor = clientData[1];
        // clientData[2] bitPrecision not used here
        this.datatypeClass = clientData[3];
        this.datatypeSize = clientData[4];
        this.signed = clientData[5];
        this.byteOrder = clientData[7];
        if (byteOrder != 1) {
            throw new UnsupportedOperationException("Only little-endian supported");
        }
        if (scaleType == 1) {
            throw new UnsupportedOperationException("E-scale not implemented");
        }
        if (datatypeSize != 4 && datatypeSize != 8) {
            throw new UnsupportedOperationException("Unsupported datatype size");
        }
    }

    /**
     * Decompresses the contents of the input ByteBuffer using scale-offset filter.
     * The input ByteBuffer remains unchanged.
     * @param input The ByteBuffer containing scale-offset compressed data (position to limit).
     * @return A new ByteBuffer containing the decompressed data in little-endian order.
     * @throws IOException If an I/O error occurs or data size is invalid.
     */
    public ByteBuffer deflate(ByteBuffer input) throws IOException {
        int length = input.remaining();
        if (length < datatypeSize + 4) {
            throw new IOException("Input too short for scale-offset data");
        }

        // Assume input has array since read from file and position is 0
        byte[] inputArray = input.array();
        int offset = input.arrayOffset();

        // Read min_value and min_bits with little-endian
        ByteBuffer reader = ByteBuffer.wrap(inputArray, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        long minLong = 0;
        double minDouble = 0.0;
        if (scaleType == 2) {  // Integer
            if (datatypeSize == 4) {
                minLong = reader.getInt();
            } else {
                minLong = reader.getLong();
            }
        } else {  // Float D-scale
            if (datatypeSize == 4) {
                minDouble = reader.getFloat();
            } else {
                minDouble = reader.getDouble();
            }
        }
        int minBits = reader.getInt();

        // Packed data
        int packedOffset = datatypeSize + 4;
        int packedLength = length - packedOffset;
        byte[] packed = new byte[packedLength];
        System.arraycopy(inputArray, offset + packedOffset, packed, 0, packedLength);

        // Calculate numValues
        long numBits = (long) packedLength * 8;
        if (numBits % minBits != 0) {
            throw new IOException("Packed size not aligned with min_bits");
        }
        int numValues = (int) (numBits / minBits);

        // Unpack bits (little-endian bit order, LSB first)
        ByteArrayOutputStream baos = new ByteArrayOutputStream(numValues * datatypeSize);
        int bitPos = 0;
        for (int i = 0; i < numValues; i++) {
            long unpacked = 0;
            int bitsLeft = minBits;
            int shift = 0;
            while (bitsLeft > 0) {
                int byteIndex = bitPos / 8;
                int bitOffset = bitPos % 8;
                int availableBits = 8 - bitOffset;
                int bitsToTake = Math.min(availableBits, bitsLeft);
                long mask = (1L << bitsToTake) - 1;
                unpacked |= (((packed[byteIndex] & 0xFFL) >> bitOffset) & mask) << shift;
                bitsLeft -= bitsToTake;
                shift += bitsToTake;
                bitPos += bitsToTake;
            }

            // Apply scale and offset
            if (scaleType == 2) {  // Integer
                long value = unpacked + minLong;
                // Write little-endian
                for (int b = 0; b < datatypeSize; b++) {
                    baos.write((int) (value >> (b * 8)) & 0xFF);
                }
            } else {  // Float D-scale
                double value = (unpacked + minDouble) / Math.pow(10, scaleFactor);
                // Write little-endian
                ByteBuffer writer = ByteBuffer.allocate(datatypeSize).order(ByteOrder.LITTLE_ENDIAN);
                if (datatypeSize == 4) {
                    writer.putFloat((float) value);
                } else {
                    writer.putDouble(value);
                }
                baos.write(writer.array());
            }
        }

        // Return as ByteBuffer
        return ByteBuffer.wrap(baos.toByteArray());
    }
}