package com.github.karlnicholas.hdf5javalib.data;

import lombok.Getter;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

@Getter
public class HdfFixedPoint implements HdfData {
    private final byte[] bytes;
    private final int size;
    private final boolean signed;
    private final boolean littleEndian;
    private final boolean undefined;

    public HdfFixedPoint(BigInteger value, int size, boolean signed, boolean bigEndian) {
        this.size = size;
        this.signed = signed;
        this.littleEndian = !bigEndian;
        this.bytes = toSizedByteArray(value, this.size, littleEndian);
        this.undefined = false;
    }

    public HdfFixedPoint(boolean undefined, byte[] bytes, int size) {
        validateSize(size);
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.size = size;
        this.undefined = undefined;
        this.signed = false;
        this.littleEndian = false;
    }

    // Private constructor for internal usage
    private HdfFixedPoint(byte[] bytes, int size, boolean signed, boolean littleEndian) {
        validateSize(size);
        this.bytes = Arrays.copyOf(bytes, bytes.length);
        this.size = size;
        this.signed = signed;
        this.littleEndian = littleEndian;
        this.undefined = false;
    }

    /**
     * Construct 64 bit precision from value
     * @param value of instance
     * @return instance
     */
    public static HdfFixedPoint of(long value) {
        byte[] bArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
        return new HdfFixedPoint(bArray, (short)8, false, true);
    }

    // Constructor for FileChannel
    public static HdfFixedPoint readFromFileChannel(FileChannel fileChannel, int size, boolean signed) throws IOException {
        return readFromFileChannel(fileChannel, size, signed, true); // Default to little-endian
    }

    public static HdfFixedPoint readFromFileChannel(FileChannel fileChannel, int size, boolean signed, boolean littleEndian) throws IOException {
        validateSize(size);
        byte[] bytes = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        fileChannel.read(buffer);
        return getHdfFixedPoint(size, signed, littleEndian, buffer, bytes);
    }

    private static HdfFixedPoint getHdfFixedPoint(int size, boolean signed, boolean littleEndian, ByteBuffer buffer, byte[] bytes) {
        if (littleEndian && buffer.order() != ByteOrder.LITTLE_ENDIAN) {
            reverseBytesInPlace(bytes);
        } else if (!littleEndian && buffer.order() == ByteOrder.LITTLE_ENDIAN) {
            reverseBytesInPlace(bytes);
        }
        return new HdfFixedPoint(bytes, size, signed, littleEndian);
    }

    // Constructor for ByteBuffer
    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, int size, boolean signed) {
        return readFromByteBuffer(buffer, size, signed, true); // Default to little-endian
    }

    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, int size, boolean signed, boolean littleEndian) {
        validateSize(size);
        byte[] bytes = new byte[size];
        buffer.get(bytes);

        // Adjust byte order if needed
        return getHdfFixedPoint(size, signed, littleEndian, buffer, bytes);
    }

    // Factory method for undefined values
    public static HdfFixedPoint undefined(int size) {
        byte[] undefinedBytes = new byte[size];
        Arrays.fill(undefinedBytes, (byte) 0xFF);
        return new HdfFixedPoint(true, undefinedBytes, size);
    }

    // Factory method for undefined values
    public static HdfFixedPoint undefined(ByteBuffer buffer, int size) {
        byte[] undefinedBytes = new byte[size];
        buffer.get(undefinedBytes);
        return new HdfFixedPoint(true, undefinedBytes, size);
    }

    public static boolean checkUndefined(ByteBuffer buffer, int sizeOfOffsets) {
        buffer.mark();
        byte[] undefinedBytes = new byte[sizeOfOffsets];
        buffer.get(undefinedBytes);
        buffer.reset();
        for (byte undefinedByte : undefinedBytes) {
            if (undefinedByte != (byte) 0xFF) {
                return false;
            }
        }
        return true;
    }

    // Validate size
    private static void validateSize(int size) {
        if (size <= 0 || size > 8) {
            throw new IllegalArgumentException("Size must be a positive multiple of 8");
        }
    }

    // Convert BigInteger to a sized byte array
    private byte[] toSizedByteArray(BigInteger value, int byteSize, boolean littleEndian) {
        byte[] fullBytes = value.toByteArray();
        byte[] result = new byte[byteSize];

        // Copy the least significant bytes
        int copyLength = Math.min(fullBytes.length, byteSize);
        System.arraycopy(fullBytes, fullBytes.length - copyLength, result, byteSize - copyLength, copyLength);

        // Reverse for little-endian if needed
        if (littleEndian) {
            reverseBytesInPlace(result);
        }
        return result;
    }

    // Get BigInteger value
    public BigInteger getBigIntegerValue() {
        if (isUndefined()) {
            throw new IllegalStateException("FixedPoint undefined");
        }
        byte[] effectiveBytes = littleEndian ? reverseBytes(bytes) : bytes;

        return signed ? new BigInteger(effectiveBytes) : new BigInteger(1, effectiveBytes);
    }

    // Get bytes for HDF storage
    public byte[] getHdfBytes(boolean desiredLittleEndian) {
        if (desiredLittleEndian == littleEndian) {
            return Arrays.copyOf(bytes, bytes.length);
        }
        return reverseBytes(bytes);
    }

    // Reverse bytes for endianness
    private byte[] reverseBytes(byte[] input) {
        byte[] reversed = Arrays.copyOf(input, input.length);
        reverseBytesInPlace(reversed);
        return reversed;
    }

    // Reverse bytes in place
    private static void reverseBytesInPlace(byte[] input) {
        int i = 0, j = input.length - 1;
        while (i < j) {
            byte temp = input[i];
            input[i] = input[j];
            input[j] = temp;
            i++;
            j--;
        }
    }

    @Override
    public String toString() {
        return isUndefined() ? "\"Value undefined\"" : getBigIntegerValue().toString();
    }

    @Override
    public short getSizeMessageData() {
        return (short)bytes.length;
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(bytes);
    }
}
