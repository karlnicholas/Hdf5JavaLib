package com.github.karlnicholas.hdf5javalib.data;

import lombok.Getter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

@Getter
public class HdfFixedPoint implements HdfData {
    private final byte[] bytes;
    private final int size;          // Size in bytes
    private final boolean littleEndian; // Byte order (false = big-endian)
    private final boolean lopad;     // Low padding (true = 1s, false = 0s)
    private final boolean hipad;     // High padding (true = 1s, false = 0s)
    private final boolean signed;    // 2's complement if true
    private final int bitOffset;     // Bits to the right of value (0-7)
    private final int bitPrecision;  // Number of significant bits

    public HdfFixedPoint(byte[] bytes, int size,
                         boolean littleEndian, boolean lopad, boolean hipad, boolean signed,
                         short bitOffset, short bitPrecision) {
        this.bytes = bytes.clone();
        this.size = size;
        this.signed = signed;
        this.littleEndian = littleEndian;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
        this.hipad = hipad;
        this.lopad = lopad;
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

//    public HdfFixedPoint(BigInteger value, int size, boolean signed, boolean bigEndian) {
//        this.size = size;
//        this.signed = signed;
//        this.littleEndian = !bigEndian;
//        this.bytes = toSizedByteArray(value, this.size, littleEndian);
//        this.undefined = false;
//    }
//
//    public HdfFixedPoint(boolean undefined, byte[] bytes, int size) {
//        validateSize(size);
//        this.bytes = Arrays.copyOf(bytes, bytes.length);
//        this.size = size;
//        this.undefined = undefined;
//        this.signed = false;
//        this.littleEndian = false;
//    }
//
//    // Private constructor for internal usage
//    private HdfFixedPoint(byte[] bytes, int size, boolean signed, boolean littleEndian) {
//        validateSize(size);
//        this.bytes = Arrays.copyOf(bytes, bytes.length);
//        this.size = size;
//        this.signed = signed;
//        this.littleEndian = littleEndian;
//        this.undefined = false;
//    }

    /**
     * Construct 64 bit precision from value
     * @param value of instance
     * @return instance
     */
    public static HdfFixedPoint of(long value) {
        byte[] bArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
        return new HdfFixedPoint(bArray, (short)8, true, false, false, false, (short)0, (short)0);
    }

    // Constructor for FileChannel
    public static HdfFixedPoint readFromFileChannel(FileChannel fileChannel, int size, boolean signed) throws IOException {
        return readFromFileChannel(fileChannel, size, signed, true); // Default to little-endian
    }

    public static HdfFixedPoint readFromFileChannel(FileChannel fileChannel, int size, boolean signed, boolean littleEndian) throws IOException {
        validateSize(size);
        byte[] bytes = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

        fileChannel.read(buffer);
        return getHdfFixedPoint(size, signed, littleEndian, bytes);
    }

    private static HdfFixedPoint getHdfFixedPoint(int size, boolean signed, boolean littleEndian, byte[] bytes) {
        return new HdfFixedPoint(bytes, size, littleEndian, false, false,  signed, (short) 0, (short) 0);
    }

    // Constructor for ByteBuffer
    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, int size, boolean signed) {
        return readFromByteBuffer(buffer, size, signed, buffer.order() == ByteOrder.LITTLE_ENDIAN);
    }

    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, int size, boolean signed, boolean littleEndian) {
        validateSize(size);
        byte[] bytes = new byte[size];
        buffer.get(bytes);

        // Adjust byte order if needed
        return getHdfFixedPoint(size, signed, littleEndian, bytes);
    }

    // Factory method for undefined values
    public static HdfFixedPoint undefined(int size) {
        byte[] undefinedBytes = new byte[size];
        Arrays.fill(undefinedBytes, (byte) 0xFF);
        return new HdfFixedPoint(undefinedBytes, size, false, false, false, true, (short)0, (short)0);
    }

    // Factory method for undefined values
    public static HdfFixedPoint undefined(ByteBuffer buffer, int size) {
        byte[] undefinedBytes = new byte[size];
        buffer.get(undefinedBytes);
        return new HdfFixedPoint(undefinedBytes, size, false, false, false, true, (short)0, (short)0);
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

    public boolean isUndefined() {
        for (byte undefinedByte : bytes) {
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

    public BigInteger toBigInteger() {
        if (bytes == null || bytes.length < size) {
            throw new IllegalArgumentException("Byte array too small for specified size");
        }
        if (bitOffset < 0) {
            throw new IllegalArgumentException("Invalid bitOffset");
        }
        int effectivePrecision = (bitPrecision <= 0) ? size * 8 : bitPrecision;
        if (effectivePrecision > size * 8) {
            throw new IllegalArgumentException("Bit precision exceeds available bits");
        }

        byte[] workingBytes = new byte[size];
        System.arraycopy(bytes, 0, workingBytes, 0, size);

        if (littleEndian) {
            for (int i = 0; i < size / 2; i++) {
                byte temp = workingBytes[i];
                workingBytes[i] = workingBytes[size - 1 - i];
                workingBytes[size - 1 - i] = temp;
            }
        }

        BigInteger value;
        if (!signed && workingBytes.length > 0 && (workingBytes[0] & 0x80) != 0) {
            byte[] unsignedBytes = new byte[workingBytes.length + 1];
            System.arraycopy(workingBytes, 0, unsignedBytes, 1, workingBytes.length);
            unsignedBytes[0] = 0;
            value = new BigInteger(unsignedBytes);
        } else {
            value = new BigInteger(workingBytes);
        }

        if (bitPrecision > 0) {
            int totalBits = size * 8;
            int startBit = totalBits - effectivePrecision;

            // Extract precision bits
            BigInteger precisionValue = value.shiftRight(startBit);
            BigInteger mask = BigInteger.ONE.shiftLeft(effectivePrecision).subtract(BigInteger.ONE);
            precisionValue = precisionValue.and(mask);

            // Apply signedness only if no padding
            if (signed && !hipad && !lopad && precisionValue.testBit(effectivePrecision - 1)) {
                precisionValue = precisionValue.subtract(BigInteger.ONE.shiftLeft(effectivePrecision));
            }

            // Build value with padding only if hipad or lopad is true
            value = precisionValue;
            if (hipad && startBit > 0) {
                BigInteger hiMask = BigInteger.ONE.shiftLeft(startBit).subtract(BigInteger.ONE).shiftLeft(effectivePrecision);
                value = value.or(hiMask);
            }
            if (lopad && startBit > 0) {
                BigInteger loMask = BigInteger.ONE.shiftLeft(startBit).subtract(BigInteger.ONE);
                value = value.or(loMask);
            }

            // Apply signedness to final value if padded
            if (signed && (hipad || lopad)) {
                BigInteger totalMask = BigInteger.ONE.shiftLeft(totalBits).subtract(BigInteger.ONE);
                value = value.and(totalMask);
                if (value.testBit(totalBits - 1)) {
                    value = value.subtract(BigInteger.ONE.shiftLeft(totalBits));
                }
            }
        }

        return value;
    }

    public BigDecimal toBigDecimal() {
        BigInteger intValue = toBigInteger();
        return new BigDecimal(intValue, bitOffset);
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
        return toBigDecimal().toString();
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
