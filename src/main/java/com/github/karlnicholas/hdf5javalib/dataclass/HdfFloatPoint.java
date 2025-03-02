package com.github.karlnicholas.hdf5javalib.dataclass;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class HdfFloatPoint implements HdfData {
    private final byte[] bytes;
    private final short size;
    private final boolean littleEndian;

    public HdfFloatPoint(byte[] bytes, short size) {
        this(bytes, size, true); // Defaults to little-endian
    }

    public HdfFloatPoint(byte[] bytes, short size, boolean littleEndian) {
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array cannot be null");
        }
        if (size != 32 && size != 64) {
            throw new IllegalArgumentException("Only 32-bit and 64-bit floats are supported");
        }
        if (bytes.length != size / 8) {
            throw new IllegalArgumentException(
                    "Byte array size does not match specified size. Expected: " + (size / 8) + ", Found: " + bytes.length
            );
        }
        this.bytes = Arrays.copyOf(bytes, bytes.length); // Defensive copy
        this.size = size;
        this.littleEndian = littleEndian;
    }

    public BigDecimal getBigDecimalValue() {
        if (size == 32) {
            return BigDecimal.valueOf(getFloatValue());
        } else {
            return BigDecimal.valueOf(getDoubleValue());
        }
    }

    public float getFloatValue() {
        if (size != 32) {
            throw new IllegalStateException("This is not a 32-bit float");
        }
        return readBuffer().getFloat(0);
    }

    public double getDoubleValue() {
        if (size != 64) {
            throw new IllegalStateException("This is not a 64-bit double");
        }
        return readBuffer().getDouble(0);
    }

    public byte[] getHdfBytes(boolean desiredLittleEndian) {
        if (desiredLittleEndian == littleEndian) {
            return Arrays.copyOf(bytes, bytes.length);
        }
        return reverseBytes(bytes);
    }

    private ByteBuffer readBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
        return buffer;
    }

    private byte[] reverseBytes(byte[] input) {
        byte[] reversed = new byte[input.length];
        for (int i = 0; i < input.length; i++) {
            reversed[i] = input[input.length - i - 1];
        }
        return reversed;
    }

    @Override
    public String toString() {
        if (size == 32) {
            return String.format("HdfFloatPoint{value=%.7f, size=%d, littleEndian=%b, bytes=%s}",
                    getFloatValue(), size, littleEndian, Arrays.toString(bytes));
        } else {
            return String.format("HdfFloatPoint{value=%.15f, size=%d, littleEndian=%b, bytes=%s}",
                    getDoubleValue(), size, littleEndian, Arrays.toString(bytes));
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
