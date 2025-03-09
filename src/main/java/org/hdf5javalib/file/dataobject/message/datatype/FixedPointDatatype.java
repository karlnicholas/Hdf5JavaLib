package org.hdf5javalib.file.dataobject.message.datatype;


import lombok.Getter;
import org.hdf5javalib.utils.HdfReadUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.BitSet;

@Getter
public class FixedPointDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitOffset;
    private final short bitPrecision;

    public FixedPointDatatype(byte classAndVersion, BitSet classBitField, int size, short bitOffset, short bitPrecision) {

        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
    }

    public static FixedPointDatatype parseFixedPointType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {

        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();

//        short messageDataSize;
//        if ( name.length() > 0 ) {
//            int padding = (8 -  ((name.length()+1)% 8)) % 8;
//            messageDataSize = (short) (name.length()+1 + padding + 44);
//        } else {
//            messageDataSize = 44;
//        }
//        short messageDataSize = 8;

        return new FixedPointDatatype(classAndVersion, classBitField, size, bitOffset, bitPrecision);
    }

    public static BitSet createClassBitField(boolean bigEndian, boolean loPad, boolean hiPad, boolean signed) {
        BitSet classBitField = new BitSet();
        if (bigEndian) classBitField.set(0);
        if (loPad) classBitField.set(1);
        if (hiPad) classBitField.set(2);
        if (signed) classBitField.set(3);
        return classBitField;
    }

    public static byte createClassAndVersion() {
        return 0x10;
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort(bitOffset);         // 4
        buffer.putShort(bitPrecision);      // 4
    }

    @Override
    public DatatypeClass getDatatypeClass() {
            return DatatypeClass.FIXED;
    }

    @Override
    public short getSizeMessageData() {
        return (short) size;
    }

    @Override
    public String toString() {
        return "FixedPointDatatype{" +
                "classAndVersion=" + classAndVersion +
                ", littleEndian=" + !isBigEndian() +
                ", loPad=" + isLoPad() +
                ", hiPad=" + isHiPad() +
                ", signed=" + isSigned() +
                ", size=" + size +
                ", bitOffset=" + bitOffset +
                ", bitPrecision=" + bitPrecision +
                '}';
    }

    public boolean isBigEndian() {
        return classBitField.get(0);
    }

    public boolean isLoPad() {
        return classBitField.get(1);
    }

    public boolean isHiPad() {
        return classBitField.get(2);
    }

    public boolean isSigned() {
        return classBitField.get(3);
    }

    public BigInteger toBigInteger(byte[] bytes) {
        if (bytes.length < size) {
            throw new IllegalArgumentException("Byte array too small for specified size");
        }
        if (bitOffset < 0) {
            throw new IllegalArgumentException("Invalid bitOffset");
        }
        boolean isBigEndian = isBigEndian();
        boolean isLoPad = isLoPad();
        boolean isHiPad = isHiPad();
        boolean isSigned = isSigned();
        int effectivePrecision = (bitPrecision <= 0) ? size * 8 : bitPrecision;
        if (effectivePrecision > size * 8) {
            throw new IllegalArgumentException("Bit precision exceeds available bits");
        }

        byte[] workingBytes = bytes.clone();
        if (!isBigEndian) {
            HdfReadUtils.reverseBytesInPlace(workingBytes);
        }

        BigInteger value;
        if (!isSigned && workingBytes.length > 0 && (workingBytes[0] & 0x80) != 0) {
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

            BigInteger precisionValue = value.shiftRight(startBit);
            BigInteger mask = BigInteger.ONE.shiftLeft(effectivePrecision).subtract(BigInteger.ONE);
            precisionValue = precisionValue.and(mask);

            if (isSigned && !isHiPad && !isLoPad && precisionValue.testBit(effectivePrecision - 1)) {
                precisionValue = precisionValue.subtract(BigInteger.ONE.shiftLeft(effectivePrecision));
            }

            value = precisionValue;
            if (isHiPad && startBit > 0) {
                BigInteger hiMask = BigInteger.ONE.shiftLeft(startBit).subtract(BigInteger.ONE).shiftLeft(effectivePrecision);
                value = value.or(hiMask);
            }
            if (isLoPad && startBit > 0) {
                BigInteger loMask = BigInteger.ONE.shiftLeft(startBit).subtract(BigInteger.ONE);
                value = value.or(loMask);
            }

            if (isSigned && (isHiPad || isLoPad)) {
                BigInteger totalMask = BigInteger.ONE.shiftLeft(totalBits).subtract(BigInteger.ONE);
                value = value.and(totalMask);
                if (value.testBit(totalBits - 1)) {
                    value = value.subtract(BigInteger.ONE.shiftLeft(totalBits));
                }
            }
        }

        return value;
    }

    public BigDecimal toBigDecimal(byte[] bytes) {
        boolean isBigEndian = isBigEndian();
        boolean isSigned = isSigned();
        byte[] workingBytes = bytes.clone();
        if (!isBigEndian) {
            HdfReadUtils.reverseBytesInPlace(workingBytes);
        }

        BigInteger rawValue;
        if (!isSigned && workingBytes.length > 0 && (workingBytes[0] & 0x80) != 0) {
            byte[] unsignedBytes = new byte[workingBytes.length + 1];
            System.arraycopy(workingBytes, 0, unsignedBytes, 1, workingBytes.length);
            unsignedBytes[0] = 0;
            rawValue = new BigInteger(unsignedBytes);
        } else {
            rawValue = new BigInteger(workingBytes);
        }

        // Convert to BigDecimal, shifting the binary point by bitOffset
        return new BigDecimal(rawValue)
                .divide(new BigDecimal(BigInteger.ONE.shiftLeft(bitOffset)), bitOffset, RoundingMode.HALF_UP);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        if (clazz.isAssignableFrom(BigDecimal.class)) {  // Can accept BigDecimal
            return clazz.cast(toBigDecimal(bytes));
        } else if (clazz.isAssignableFrom(BigInteger.class)) {  // Can accept BigInteger
            return clazz.cast(toBigInteger(bytes));
        } else {
            throw new UnsupportedOperationException("Unknown type: " + clazz);
        }
    }

    @Override
    public <T> T getInstance(Class<T> clazz, ByteBuffer buffer) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return getInstance(clazz, bytes);
    }
}

