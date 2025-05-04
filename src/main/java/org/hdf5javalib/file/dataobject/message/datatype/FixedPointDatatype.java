package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.utils.HdfReadUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an HDF5 Fixed-Point Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code FixedPointDatatype} class models a fixed-point number in HDF5, supporting parsing from a
 * {@link java.nio.ByteBuffer} and conversion to Java types such as {@link BigDecimal}, {@link BigInteger},
 * {@code Long}, or {@link HdfFixedPoint}. It handles byte order, padding, and signedness as per the HDF5
 * fixed-point datatype (class 0).
 * </p>
 *
 * @see org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype
 * @see org.hdf5javalib.utils.HdfReadUtils
 */
@Getter
public class FixedPointDatatype implements HdfDatatype {
    /** The class and version information for the datatype (class 0, version 1). */
    private final byte classAndVersion;
    /** A BitSet containing class-specific bit field information (byte order, padding, signedness). */
    private final BitSet classBitField;
    /** The total size of the fixed-point datatype in bytes. */
    private final int size;
    /** The bit offset of the first significant bit. */
    private final short bitOffset;
    /** The number of bits of precision. */
    private final short bitPrecision;

    /** Map of converters for transforming byte data to specific Java types. */
    private static final Map<Class<?>, HdfConverter<FixedPointDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(BigDecimal.class, (bytes, dt) -> dt.toBigDecimal(bytes));
        CONVERTERS.put(BigInteger.class, (bytes, dt) -> dt.toBigInteger(bytes));
        CONVERTERS.put(Long.class, (bytes, dt) -> dt.toLong(bytes));
        CONVERTERS.put(Integer.class, (bytes, dt) -> dt.toInteger(bytes));
        CONVERTERS.put(Short.class, (bytes, dt) -> dt.toShort(bytes));
        CONVERTERS.put(Byte.class, (bytes, dt) -> dt.toByte(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfFixedPoint.class, HdfFixedPoint::new);
        CONVERTERS.put(HdfData.class, HdfFixedPoint::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    /**
     * Constructs a FixedPointDatatype representing an HDF5 fixed-point datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet containing class-specific bit field information
     * @param size            the total size of the datatype in bytes
     * @param bitOffset       the bit offset of the first significant bit
     * @param bitPrecision    the number of bits of precision
     */
    public FixedPointDatatype(byte classAndVersion, BitSet classBitField, int size, short bitOffset, short bitPrecision) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
    }

    /**
     * Parses an HDF5 fixed-point datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet containing class-specific bit field information
     * @param size            the total size of the datatype in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new FixedPointDatatype instance parsed from the buffer
     */
    public static FixedPointDatatype parseFixedPointType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();
        return new FixedPointDatatype(classAndVersion, classBitField, size, bitOffset, bitPrecision);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 fixed-point datatype.
     *
     * @param bigEndian true for big-endian byte order, false for little-endian
     * @param loPad     true for low padding, false otherwise
     * @param hiPad     true for high padding, false otherwise
     * @param signed    true if the number is signed, false if unsigned
     * @return a BitSet encoding byte order, padding, and signedness
     */
    public static BitSet createClassBitField(boolean bigEndian, boolean loPad, boolean hiPad, boolean signed) {
        BitSet classBitField = new BitSet();
        if (bigEndian) classBitField.set(0);
        if (loPad) classBitField.set(1);
        if (hiPad) classBitField.set(2);
        if (signed) classBitField.set(3);
        return classBitField;
    }

    /**
     * Creates a fixed class and version byte for an HDF5 fixed-point datatype.
     *
     * @return a byte representing class 0 and version 1, as defined by the HDF5 specification
     */
    @SuppressWarnings("SameReturnValue")
    public static byte createClassAndVersion() {
        return 0x10;
    }

    /**
     * Creates an undefined HdfFixedPoint instance with all bits set to 1.
     *
     * @return an HdfFixedPoint instance representing an undefined value
     */
    public HdfFixedPoint undefined() {
        HdfReadUtils.validateSize(size);
        byte[] undefinedBytes = new byte[size];
        Arrays.fill(undefinedBytes, (byte) 0xFF);
        return new HdfFixedPoint(undefinedBytes, this);
    }

    /**
     * Creates an undefined HdfFixedPoint instance from a ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the undefined value data
     * @return an HdfFixedPoint instance representing an undefined value
     */
    public HdfFixedPoint undefined(ByteBuffer buffer) {
        HdfReadUtils.validateSize(size);
        byte[] undefinedBytes = new byte[size];
        buffer.get(undefinedBytes);
        return new HdfFixedPoint(undefinedBytes, this);
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort(bitOffset);         // 2
        buffer.putShort(bitPrecision);      // 2
    }

    /**
     * Returns the datatype class for this fixed-point datatype.
     *
     * @return DatatypeClass.FIXED, indicating an HDF5 fixed-point datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.FIXED;
    }

    /**
     * Returns the size of the datatype message data.
     *
     * @return the size of the message data in bytes, as a short
     */
    @Override
    public short getSizeMessageData() {
        return (short) (8 + 8);
    }

    /**
     * Returns a string representation of this FixedPointDatatype.
     *
     * @return a string describing the datatype's class, version, byte order, padding, signedness, size, bit offset, and precision
     */
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

    /**
     * Checks if the fixed-point number uses big-endian byte order.
     *
     * @return true if big-endian, false if little-endian
     */
    public boolean isBigEndian() {
        return classBitField.get(0);
    }

    /**
     * Checks if low padding is enabled.
     *
     * @return true if low padding is enabled, false otherwise
     */
    public boolean isLoPad() {
        return classBitField.get(1);
    }

    /**
     * Checks if high padding is enabled.
     *
     * @return true if high padding is enabled, false otherwise
     */
    public boolean isHiPad() {
        return classBitField.get(2);
    }

    /**
     * Checks if the fixed-point number is signed.
     *
     * @return true if signed, false if unsigned
     */
    public boolean isSigned() {
        return classBitField.get(3);
    }

    /**
     * Converts the byte array to a Long value.
     *
     * @param bytes the byte array to convert
     * @return the Long value
     * @throws IllegalStateException if bitOffset is not zero
     * @throws IllegalArgumentException if the byte array length or size is invalid for Long
     */
    public long toLong(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Long: bitOffset must be 0, got " + bitOffset);
        }
        if (bytes.length > 8 || size > 8 || bytes.length != size) {
            throw new IllegalArgumentException("Bytes or size wrong for Long, got " + bytes.length+":"+size);
        }

        long value = 0;
        if (isBigEndian()) {
            for (int i = 0; i < size; i++) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        } else {
            for (int i = size-1; i >= 0; i--) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        }
        return value;
    }

    /**
     * Converts the byte array to an Integer value.
     *
     * @param bytes the byte array to convert
     * @return the Integer value
     * @throws IllegalStateException if bitOffset is not zero
     * @throws IllegalArgumentException if the byte array length or size is invalid for Integer
     */
    public int toInteger(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Integer: bitOffset must be 0, got " + bitOffset);
        }
        if (bytes.length > 4 || size > 4 || bytes.length != size) {
            throw new IllegalArgumentException("Bytes or size wrong for Integer, got " + bytes.length+":"+size);
        }

        int value = 0;
        if (isBigEndian()) {
            for (int i = 0; i < size; i++) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        } else {
            for (int i = size-1; i >= 0; i--) {
                value = (value << 8) | (bytes[i] & 0xFF);
            }
        }

        return value;
    }

    /**
     * Converts the byte array to a Short value.
     *
     * @param bytes the byte array to convert
     * @return the Short value
     * @throws IllegalStateException if bitOffset is not zero
     * @throws IllegalArgumentException if the byte array length or size is invalid for Short
     */
    public short toShort(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Short: bitOffset must be 0, got " + bitOffset);
        }
        if (bytes.length > 2 || size > 2 || bytes.length != size) {
            throw new IllegalArgumentException("Bytes or size wrong for Short, got " + bytes.length+":"+size);
        }

        short value = 0;
        if (isBigEndian()) {
            for (int i = 0; i < size; i++) {
                value = (short) ((value << 8) | (bytes[i] & 0xFF));
            }
        } else {
            for (int i = size-1; i >= 0; i--) {
                value = (short) ((value << 8) | (bytes[i] & 0xFF));
            }
        }
        return value;
    }

    /**
     * Converts the byte array to a Byte value.
     *
     * @param bytes the byte array to convert
     * @return the Byte value
     * @throws IllegalStateException if bitOffset is not zero
     * @throws IllegalArgumentException if the byte array length or size is invalid for Byte
     */
    public byte toByte(byte[] bytes) {
        if (bitOffset != 0) {
            throw new IllegalStateException("Cannot convert to Byte: bitOffset must be 0, got " + bitOffset);
        }
        if (bytes.length > 1 || size > 1 || bytes.length != size) {
            throw new IllegalArgumentException("Bytes or size wrong for Byte, got " + bytes.length+":"+size);
        }

        return bytes[0];
    }

    /**
     * Converts the byte array to a BigInteger value, handling byte order, padding, and signedness.
     *
     * @param bytes the byte array to convert
     * @return the BigInteger value
     * @throws IllegalArgumentException if the byte array size, bit offset, or bit precision is invalid
     */
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

    /**
     * Converts the byte array to a BigDecimal value, adjusting for bit offset.
     *
     * @param bytes the byte array to convert
     * @return the BigDecimal value
     */
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

    /**
     * Registers a converter for transforming FixedPointDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the HdfConverter for converting between FixedPointDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, HdfConverter<FixedPointDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    /**
     * Converts byte data to an instance of the specified class using registered converters.
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @param bytes the byte array containing the data
     * @return an instance of type T created from the byte array
     * @throws UnsupportedOperationException if no suitable converter is found
     */
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

    /**
     * Indicates whether a global heap is required for this datatype.
     *
     * @param required true if the global heap is required, false otherwise
     * @return false, as FixedPointDatatype does not require a global heap
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required | false;
    }

    /**
     * Sets the global heap for this datatype (no-op for FixedPointDatatype).
     *
     * @param grok the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap grok) {}

    /**
     * Converts the byte array to a string representation using BigDecimal.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the fixed-point value
     */
    @Override
    public String toString(byte[] bytes) {
        return toBigDecimal(bytes).toString();
    }

    /**
     * Returns the class and version byte for this datatype.
     *
     * @return the class and version byte
     */
    @Override
    public byte getClassAndVersion() {
        return classAndVersion;
    }

    /**
     * Returns the total size of the fixed-point datatype in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }

    /**
     * Returns the class bit field for this datatype.
     *
     * @return the BitSet containing class-specific bit field information
     */
    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }
}