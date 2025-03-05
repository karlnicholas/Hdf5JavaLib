package org.hdf5javalib.dataclass;

import lombok.Getter;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;

@Getter
public class HdfFixedPoint implements HdfData {
    private final byte[] bytes;
    private final int size;          // Size in bytes
    private final boolean bigEndian; // Byte order (false = big-endian)
    private final boolean loPad;     // Low padding (true = 1s, false = 0s)
    private final boolean hiPad;     // High padding (true = 1s, false = 0s)
    private final boolean signed;    // 2's complement if true
    private final int bitOffset;     // Bits to the right of value (0-7)
    private final int bitPrecision;  // Number of significant bits

    public HdfFixedPoint(byte[] bytes, int size,
                         boolean bigEndian, boolean loPad, boolean hiPad, boolean signed,
                         short bitOffset, short bitPrecision) {
        if (bytes == null || bytes.length < size) {
            throw new IllegalArgumentException("Byte array too small for specified size");
        }
        if (bitOffset < 0) {
            throw new IllegalArgumentException("Invalid bitOffset");
        }
        this.bytes = bytes.clone();
        this.size = size;
        this.signed = signed;
        this.bigEndian = bigEndian;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
        this.hiPad = hiPad;
        this.loPad = loPad;
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public HdfFixedPoint(BigInteger value, int size,
                         boolean bigEndian, boolean loPad, boolean hiPad, boolean signed,
                         short bitOffset, short bitPrecision) {
        this(toSizedByteArray(value, size, bigEndian), size, bigEndian, loPad, hiPad, signed, bitOffset, bitPrecision);
    }

    public HdfFixedPoint(BigInteger value, int size, boolean signed, boolean bigEndian) {
        this(toSizedByteArray(value, size, bigEndian), size, bigEndian, false, false, signed, (short) 0, (short) (size*8));
    }
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
        return new HdfFixedPoint(bArray, (short)8, false, false, false, false, (short)0, (short)64);
    }

    public static HdfFixedPoint readFromFileChannel(FileChannel fileChannel, int size, BitSet classBitField, short bitOffset, short bitPrecision) throws IOException {
        validateSize(size);
        byte[] bytes = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(classBitField.get(0) ? ByteOrder.BIG_ENDIAN: ByteOrder.LITTLE_ENDIAN );

        fileChannel.read(buffer);
        return getHdfFixedPoint(buffer.array(), size, classBitField, bitOffset, bitPrecision);
    }

    private static HdfFixedPoint getHdfFixedPoint(byte[] bytes, int size, BitSet classBitField, short bitOffset, short bitPrecision) {
        return new HdfFixedPoint(bytes, size, classBitField.get(0), classBitField.get(1), classBitField.get(2),  classBitField.get(3), bitOffset, bitPrecision);
    }

    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, int size, BitSet classBitField, short bitOffset, short bitPrecision) {
        validateSize(size);
        byte[] bytes = new byte[size];
        buffer.get(bytes);

        // Adjust byte order if needed
        return getHdfFixedPoint(bytes, size, classBitField, bitOffset, bitPrecision);
    }

    // Factory method for undefined values
    public static HdfFixedPoint undefined(int size) {
        byte[] undefinedBytes = new byte[size];
        Arrays.fill(undefinedBytes, (byte) 0xFF);
        return new HdfFixedPoint(undefinedBytes, size, false, false, false, true, (short)0, (short)(size*8));
    }

    // Factory method for undefined values
    public static HdfFixedPoint undefined(ByteBuffer buffer, int size) {
        byte[] undefinedBytes = new byte[size];
        buffer.get(undefinedBytes);
        return new HdfFixedPoint(undefinedBytes, size, false, false, false, true, (short)0, (short)(size*8));
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
    public static byte[] toSizedByteArray(BigInteger value, int byteSize, boolean bigEndian) {
        byte[] fullBytes = value.toByteArray();
        byte[] result = new byte[byteSize];

        // Copy the least significant bytes
        int copyLength = Math.min(fullBytes.length, byteSize);
        System.arraycopy(fullBytes, fullBytes.length - copyLength, result, byteSize - copyLength, copyLength);

        // Reverse for little-endian if needed
        if (!bigEndian) {
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

        if (!bigEndian) {
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
            if (signed && !hiPad && !loPad && precisionValue.testBit(effectivePrecision - 1)) {
                precisionValue = precisionValue.subtract(BigInteger.ONE.shiftLeft(effectivePrecision));
            }

            // Build value with padding only if hipad or lopad is true
            value = precisionValue;
            if (hiPad && startBit > 0) {
                BigInteger hiMask = BigInteger.ONE.shiftLeft(startBit).subtract(BigInteger.ONE).shiftLeft(effectivePrecision);
                value = value.or(hiMask);
            }
            if (loPad && startBit > 0) {
                BigInteger loMask = BigInteger.ONE.shiftLeft(startBit).subtract(BigInteger.ONE);
                value = value.or(loMask);
            }

            // Apply signedness to final value if padded
            if (signed && (hiPad || loPad)) {
                BigInteger totalMask = BigInteger.ONE.shiftLeft(totalBits).subtract(BigInteger.ONE);
                value = value.and(totalMask);
                if (value.testBit(totalBits - 1)) {
                    value = value.subtract(BigInteger.ONE.shiftLeft(totalBits));
                }
            }
        }

        return value;
    }

    public BigDecimal toBigDecimal(int scale) {
        // Create raw BigInteger from bytes, respecting signed and littleEndian
        byte[] workingBytes = new byte[size];
        System.arraycopy(bytes, 0, workingBytes, 0, size);

        if (!bigEndian) {
            for (int i = 0; i < size / 2; i++) {
                byte temp = workingBytes[i];
                workingBytes[i] = workingBytes[size - 1 - i];
                workingBytes[size - 1 - i] = temp;
            }
        }

        BigInteger rawValue;
        if (!signed && workingBytes.length > 0 && (workingBytes[0] & 0x80) != 0) {
            byte[] unsignedBytes = new byte[workingBytes.length + 1];
            System.arraycopy(workingBytes, 0, unsignedBytes, 1, workingBytes.length);
            unsignedBytes[0] = 0;
            rawValue = new BigInteger(unsignedBytes);
        } else {
            rawValue = new BigInteger(workingBytes);
        }

        // Scale by 2^bitOffset to reflect HDF5 fixed-point fractional part
        return new BigDecimal(rawValue).divide(new BigDecimal(BigInteger.ONE.shiftLeft(bitOffset)), scale, RoundingMode.HALF_UP);
    }
//    public BigDecimal toBigDecimal() {
//        BigInteger intValue = toBigInteger();
//        return new BigDecimal(intValue).divide(new BigDecimal(1 << bitOffset), 10, BigDecimal.ROUND_HALF_UP);
//    }
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
        return toBigInteger().toString();
    }

    @Override
    public short getSizeMessageData() {
        return (short)bytes.length;
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        HdfWriteUtils.writeFixedPointToBuffer(buffer, this);
    }
}
