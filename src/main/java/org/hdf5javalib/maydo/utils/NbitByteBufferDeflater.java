package org.hdf5javalib.maydo.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NbitByteBufferDeflater implements ByteBufferDeflater {
    private final int bitsPerValue;
    private final int offset;
    private final boolean signed;
    private final boolean littleEndian;
    private final int datatypeSize;

    public NbitByteBufferDeflater(int[] clientData) {
        this.bitsPerValue = clientData[6];
        this.offset = clientData[7];
        this.signed = clientData[3] != 0;
        this.littleEndian = clientData[1] == 0;
        this.datatypeSize = clientData[4];
    }

    /**
     * Unpacks (decompresses) the contents of the input ByteBuffer using n-bit unpacking.
     * The input ByteBuffer remains unchanged.
     * @param input The ByteBuffer containing n-bit packed data (position to limit).
     * @return A new ByteBuffer containing the unpacked data (assumed as big-endian 32-bit integers).
     * @throws IOException If an I/O error occurs or data size is invalid.
     */
    public ByteBuffer deflate(ByteBuffer input) throws IOException {
        byte[] packedArray;
        int offset;
        int length = input.remaining();
        if (input.hasArray()) {
            packedArray = input.array();
            offset = input.arrayOffset() + input.position();
        } else {
            ByteBuffer dup = input.duplicate();
            packedArray = new byte[length];
            dup.get(packedArray);
            offset = 0;
        }

        // Skip the first byte (header)
        int packedOffset = offset + 1;
        int packedLength = length - 1;
        if (packedLength <= 0) {
            throw new IOException("Invalid input length");
        }

        // Create packed byte array, skipping header
        byte[] packed = new byte[packedLength];
        System.arraycopy(packedArray, packedOffset, packed, 0, packedLength);

        // Create BigInteger from the input bytes (big-endian interpretation)
        BigInteger bigPacked = new BigInteger(1, packed);

        // Calculate number of values
        int numBits = packedLength * 8;
        if (numBits % bitsPerValue != 0) {
            throw new IOException("Input size not aligned with bits per value");
        }
        int numValues = numBits / bitsPerValue;

        // Unpack values in reverse order (due to BigInteger low-end unpacking)
        List<Integer> valuesList = new ArrayList<>(numValues);
        BigInteger mask = BigInteger.valueOf((1L << bitsPerValue) - 1);
        for (int i = 0; i < numValues; i++) {
            int value = bigPacked.and(mask).intValue();
            // Swap bytes for correct endianness (packed as little-endian 16-bit)
            value = ((value & 0xff) << 8) | ((value >> 8) & 0xff);
            valuesList.add(value);
            bigPacked = bigPacked.shiftRight(bitsPerValue);
        }

        // Reverse the list to get original order
        Collections.reverse(valuesList);

        // Pack into output with shift, sign extend, and endianness
        ByteArrayOutputStream baos = new ByteArrayOutputStream(numValues * datatypeSize);
        for (int val : valuesList) {
            long value = val & ((1L << bitsPerValue) - 1); // Ensure unsigned treatment
            value <<= offset;
            if (signed) {
                long signMask = 1L << (offset + bitsPerValue - 1);
                if ((value & signMask) != 0) {
                    value |= (-1L << (offset + bitsPerValue));
                }
            }
            for (int b = 0; b < datatypeSize; b++) {
                int shift = littleEndian ? (b * 8) : ((datatypeSize - 1 - b) * 8);
                baos.write((int) (value >> shift) & 0xFF);
            }
        }

        // Return as ByteBuffer
        return ByteBuffer.wrap(baos.toByteArray());
    }
}