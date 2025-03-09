package org.hdf5javalib.file.dataobject.message.datatype;


import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfFloatPoint;
import org.hdf5javalib.utils.HdfReadUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class FixedPointDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitOffset;
    private final short bitPrecision;
    // In your HdfDataType/FixedPointDatatype class
    private static final Map<Class<?>, HdfConverter<FixedPointDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(BigDecimal.class, (bytes, dt) -> dt.toBigDecimal(bytes));
        CONVERTERS.put(BigInteger.class, (bytes, dt) -> dt.toBigInteger(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toBigInteger(bytes).toString());
        CONVERTERS.put(HdfFixedPoint.class, HdfFixedPoint::new);
        CONVERTERS.put(HdfData.class, HdfFixedPoint::new);
        CONVERTERS.put(Long.class, (bytes, dt) -> dt.toLong(bytes));
        CONVERTERS.put(Integer.class, (bytes, dt) -> dt.toInteger(bytes));
        CONVERTERS.put(Short.class, (bytes, dt) -> dt.toShort(bytes));
        CONVERTERS.put(Byte.class, (bytes, dt) -> dt.toByte(bytes));
    }

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

    // Conversion methods
    public long toLong(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Long: bitOffset must be 0, got " + bitOffset);
        }
        if (size != 8) {
            throw new IllegalStateException("Cannot convert to Long: size must be 8, got " + size);
        }
        if (bytes.length < 8) {
            throw new IllegalArgumentException("Byte array too small for Long, need 8 bytes, got " + bytes.length);
        }

        long value = 0;
        if (isBigEndian()) {
            for (int i = 0; i < 8; i++) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        } else {
            for (int i = 7; i >= 0; i--) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        }

        if (!isSigned() && value < 0) {
            throw new ArithmeticException("Unsigned value out of range for signed Long: " + value);
        }
        return value;
    }

    public int toInteger(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Integer: bitOffset must be 0, got " + bitOffset);
        }
        if (size != 4) {
            throw new IllegalStateException("Cannot convert to Integer: size must be 4, got " + size);
        }
        if (bytes.length < 4) {
            throw new IllegalArgumentException("Byte array too small for Integer, need 4 bytes, got " + bytes.length);
        }

        int value = 0;
        if (isBigEndian()) {
            for (int i = 0; i < 4; i++) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        } else {
            for (int i = 3; i >= 0; i--) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        }

        if (!isSigned() && value < 0) {
            throw new ArithmeticException("Unsigned value out of range for signed Integer: " + value);
        }
        return value;
    }

    public short toShort(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Short: bitOffset must be 0, got " + bitOffset);
        }
        if (size != 2) {
            throw new IllegalStateException("Cannot convert to Short: size must be 2, got " + size);
        }
        if (bytes.length < 2) {
            throw new IllegalArgumentException("Byte array too small for Short, need 2 bytes, got " + bytes.length);
        }

        short value = 0;
        if (isBigEndian()) {
            for (int i = 0; i < 2; i++) {
                value = (short) ((value << 8) | (bytes[i] & 0xFF));
            }
        } else {
            for (int i = 1; i >= 0; i--) {
                value = (short) ((value << 8) | (bytes[i] & 0xFF));
            }
        }

        if (!isSigned() && value < 0) {
            throw new ArithmeticException("Unsigned value out of range for signed Short: " + value);
        }
        return value;
    }

    public byte toByte(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Byte: bitOffset must be 0, got " + bitOffset);
        }
        if (size != 1) {
            throw new IllegalStateException("Cannot convert to Byte: size must be 1, got " + size);
        }
        if (bytes.length < 1) {
            throw new IllegalArgumentException("Byte array too small for Byte, need 1 byte, got " + bytes.length);
        }

        byte value = bytes[0];
        if (!isSigned() && value < 0) {
            throw new ArithmeticException("Unsigned value out of range for signed Byte: " + value);
        }
        return value;
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

    // Public method to add user-defined converters
    public static <T> void addConverter(Class<T> clazz, HdfConverter<FixedPointDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<FixedPointDatatype, T> converter = (HdfConverter<FixedPointDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<FixedPointDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

//    @Override
//    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
//        if (BigDecimal.class.isAssignableFrom(clazz)) {  // Accepts BigDecimal or subclasses
//            return clazz.cast(toBigDecimal(bytes));
//        } else if (BigInteger.class.isAssignableFrom(clazz)) {  // Accepts BigInteger or subclasses
//            return clazz.cast(toBigInteger(bytes));
//        } else if (String.class.isAssignableFrom(clazz)) {  // Accepts String or subclasses
//            return clazz.cast(toBigInteger(bytes).toString());
//        } else if (HdfFixedPoint.class.isAssignableFrom(clazz)) {  // Accepts HdfFixedPoint or subclasses
//            return clazz.cast(new HdfFixedPoint(bytes, this));
//        } else if (Long.class.isAssignableFrom(clazz)) {  // Accepts Long or subclasses
//            return clazz.cast(Long.valueOf(toLong(bytes)));
//        } else if (Integer.class.isAssignableFrom(clazz)) {  // Accepts Integer or subclasses
//            return clazz.cast(Integer.valueOf(toInteger(bytes)));
//        } else if (Short.class.isAssignableFrom(clazz)) {  // Accepts Short or subclasses
//            return clazz.cast(Short.valueOf(toShort(bytes)));
//        } else if (Byte.class.isAssignableFrom(clazz)) {  // Accepts Byte or subclasses
//            return clazz.cast(Byte.valueOf(toByte(bytes)));
//        } else {
//            throw new UnsupportedOperationException("Unknown type: " + clazz);
//        }
//    }

    @Override
    public <T> T getInstance(Class<T> clazz, ByteBuffer buffer) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return getInstance(clazz, bytes);
    }
}

