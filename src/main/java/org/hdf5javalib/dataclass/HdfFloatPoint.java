package org.hdf5javalib.dataclass;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

public class HdfFloatPoint implements HdfData {
    private final byte[] bytes;
//    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitOffset;
    private final short bitPrecision;
    private final byte exponentLocation;
    private final byte exponentSize;
    private final byte mantissaLocation;
    private final byte mantissaSize;
    private final int exponentBias;

    public HdfFloatPoint(byte[] bytes, BitSet classBitField, int size, short bitOffset, short bitPrecision, byte exponentLocation, byte exponentSize, byte mantissaLocation, byte mantissaSize, int exponentBias) {
        this.bytes = bytes;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
        this.exponentLocation = exponentLocation;
        this.exponentSize = exponentSize;
        this.mantissaLocation = mantissaLocation;
        this.mantissaSize = mantissaSize;
        this.exponentBias = exponentBias;
    }

//    public BigDecimal getBigDecimalValue() {
//        if (size == 32) {
//            return BigDecimal.valueOf(getFloatValue());
//        } else {
//            return BigDecimal.valueOf(getDoubleValue());
//        }
//    }

    // Convert buffer to Float
    public Float toFloat() {
        double value = toDoubleValue(bytes);
        return (float) value; // Cast to float, may lose precision if size > 4 bytes
    }

    // Convert buffer to Double
    public Double toDouble() {
        return toDoubleValue(bytes);
    }

    private double toDoubleValue(byte[] buffer) {
        if (buffer.length != size) {
            throw new IllegalArgumentException("Buffer size (" + buffer.length + ") must match datatype size (" + size + ")");
        }

        // Determine byte order from classBitField
        ByteOrder order = getByteOrder();
        ByteBuffer bb = ByteBuffer.wrap(buffer).order(order);

        // Convert buffer to long (up to 64 bits) for bit manipulation
        long bits = 0;
        if (size <= 4) {
            bits = bb.getInt() & 0xFFFFFFFFL; // 32-bit unsigned
        } else if (size <= 8) {
            bits = bb.getLong();
        } else {
            throw new UnsupportedOperationException("Size > 8 bytes not supported");
        }

        // Shift bits to align with bitOffset
        bits >>>= bitOffset; // Unsigned right shift to discard lower bits

        // Extract sign bit (from signLocation in classBitField bits 8-15)
        int signLocation = getSignLocation();
        long signMask = 1L << signLocation;
        int sign = (bits & signMask) != 0 ? -1 : 1;

        // Extract exponent
        long exponentMask = (1L << exponentSize) - 1;
        long rawExponent = (bits >>> exponentLocation) & exponentMask;
        int exponent = (int) (rawExponent - exponentBias);

        // Extract mantissa
        long mantissaMask = (1L << mantissaSize) - 1;
        long mantissa = (bits >>> mantissaLocation) & mantissaMask;

        // Assume normalized number (implied leading 1, common in HDF5 float spec)
        double mantissaValue = 1.0 + (double) mantissa / (1L << mantissaSize); // Normalize mantissa

        // Combine: sign * mantissa * 2^exponent
        double value = sign * mantissaValue * Math.pow(2, exponent);

        return value;
    }

    private ByteOrder getByteOrder() {
        boolean bit0 = classBitField.get(0);
        boolean bit6 = classBitField.get(6);
        if (!bit6 && !bit0) return ByteOrder.LITTLE_ENDIAN;
        if (!bit6 && bit0) return ByteOrder.BIG_ENDIAN;
        if (bit6 && !bit0) throw new IllegalArgumentException("Reserved byte order");
        if (bit6 && bit0) throw new UnsupportedOperationException("VAX-endian not supported");
        return ByteOrder.LITTLE_ENDIAN; // Default, should never reach here
    }

    private int getSignLocation() {
        int signLoc = 0;
        for (int i = 8; i <= 15; i++) {
            if (classBitField.get(i)) {
                signLoc |= (1 << (i - 8));
            }
        }
        return signLoc;
    }
//    public byte[] getHdfBytes(boolean desiredLittleEndian) {
//        if (desiredLittleEndian == littleEndian) {
//            return Arrays.copyOf(bytes, bytes.length);
//        }
//        return reverseBytes(bytes);
//    }
//
//    private ByteBuffer readBuffer() {
//        ByteBuffer buffer = ByteBuffer.wrap(bytes);
//        buffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
//        return buffer;
//    }
//
//    private byte[] reverseBytes(byte[] input) {
//        byte[] reversed = new byte[input.length];
//        for (int i = 0; i < input.length; i++) {
//            reversed[i] = input[input.length - i - 1];
//        }
//        return reversed;
//    }

    @Override
    public String toString() {
        if (size == 4) {
            return toFloat().toString();
        } else {
            return toDouble().toString();
        }
    }

    @Override
    public short getSizeMessageData() {
        return (short)bytes.length;
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {

    }
}
