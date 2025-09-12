package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * A helper class to read various data types from a ByteBuffer,
 * assuming Little Endian byte order as is standard for HDF5.
 */
public class DataReader {
    private final ByteBuffer buffer;

    public DataReader(ByteBuffer buffer) {
        this.buffer = buffer;
        this.buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    public byte readByte() {
        return buffer.get();
    }

    public short readShort() {
        return buffer.getShort();
    }

    public int readInt() {
        return buffer.getInt();
    }

    public long readLong() {
        return buffer.getLong();
    }

    public byte[] readBytes(int length) {
        byte[] dst = new byte[length];
        buffer.get(dst);
        return dst;
    }

    public String readString(int length) {
        return new String(readBytes(length), StandardCharsets.US_ASCII);
    }

    /**
     * Reads a variable-sized value (like an offset or length) and returns it as a long.
     * @param size The number of bytes to read (e.g., 4 for a 32-bit value, 8 for 64-bit).
     * @return The value read, promoted to a long.
     */
    public long readSizedValue(int size) {
        switch (size) {
            case 1:
                return readByte() & 0xFFL;
            case 2:
                return readShort() & 0xFFFFL;
            case 4:
                return readInt() & 0xFFFFFFFFL;
            case 8:
                return readLong();
            default:
                throw new IllegalArgumentException("Unsupported size for sized value: " + size);
        }
    }

    public int position() {
        return buffer.position();
    }

    public void position(int newPosition) {
        buffer.position(newPosition);
    }
}