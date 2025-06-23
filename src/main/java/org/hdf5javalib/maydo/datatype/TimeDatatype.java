package org.hdf5javalib.maydo.datatype;

import org.hdf5javalib.maydo.dataclass.HdfData;
import org.hdf5javalib.maydo.dataclass.HdfTime;
import org.hdf5javalib.maydo.hdffile.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.infrastructure.HdfGlobalHeap;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents an HDF5 Time Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code TimeDatatype} class models a time datatype in HDF5, representing time values as integers.
 * It supports parsing from a {@link ByteBuffer} and conversion to Java types such as
 * {@link Long}, {@link BigInteger}, {@link HdfTime}, or {@code String}, as per the HDF5 time datatype
 * (class 2).
 * </p>
 *
 * @see Datatype
 * @see HdfGlobalHeap
 */
public class TimeDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 2, version 1).
     */
    private final int classAndVersion;
    /**
     * A BitSet containing class-specific bit field information (byte order).
     */
    private final BitSet classBitField;
    /**
     * The size of the time data in bytes.
     */
    private final int size;
    /**
     * The number of bits of precision.
     */
    private final int bitPrecision;
    private final HdfDataFile dataFile;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<TimeDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(Long.class, (bytes, dt) -> dt.toLong(bytes));
        CONVERTERS.put(BigInteger.class, (bytes, dt) -> dt.toBigInteger(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfTime.class, HdfTime::new);
        CONVERTERS.put(HdfData.class, HdfTime::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes); // Raw reference bytes
    }

    /**
     * Constructs a TimeDatatype representing an HDF5 time datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet containing class-specific bit field information
     * @param size            the size of the time data in bytes
     * @param bitPrecision    the number of bits of precision
     * @param dataFile        datafile
     */
    public TimeDatatype(int classAndVersion, BitSet classBitField, int size, int bitPrecision, HdfDataFile dataFile) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitPrecision = bitPrecision;
        this.dataFile = dataFile;
    }

    /**
     * Parses an HDF5 time datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet containing class-specific bit field information
     * @param size            the size of the time data in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new TimeDatatype instance parsed from the buffer
     */
    public static TimeDatatype parseTimeType(int classAndVersion, BitSet classBitField, int size, ByteBuffer buffer, HdfDataFile dataFile) {
        int bitPrecision = Short.toUnsignedInt(buffer.getShort());
        return new TimeDatatype(classAndVersion, classBitField, size, bitPrecision, dataFile);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 time datatype.
     *
     * @param bigEndian true for big-endian byte order, false for little-endian
     * @return a 24-bit BitSet encoding the byte order
     */
    public static BitSet createClassBitField(boolean bigEndian) {
        BitSet bits = new BitSet(24);
        bits.set(0, bigEndian);
        return bits;
    }

    /**
     * Creates a fixed class and version byte for an HDF5 time datatype.
     *
     * @return a byte representing class 2 and version 1, as defined by the HDF5 specification
     */
    public static byte createClassAndVersion() {
        return 0x12;
    }

    /**
     * Registers a converter for transforming TimeDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between TimeDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<TimeDatatype, T> converter) {
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
        DatatypeConverter<TimeDatatype, T> converter = (DatatypeConverter<TimeDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<TimeDatatype, ?>> entry : CONVERTERS.entrySet()) {
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
     * @return false, as TimeDatatype does not require a global heap
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required;
    }

    /**
     * Checks if the time datatype uses big-endian byte order.
     *
     * @return true if big-endian, false if little-endian
     */
    public boolean isBigEndian() {
        return classBitField.get(0);
    }

    /**
     * Converts the byte array to a Long value.
     *
     * @param bytes the byte array to convert
     * @return the Long value
     * @throws IllegalArgumentException if the byte array exceeds 8 bytes
     */
    public Long toLong(byte[] bytes) {
        if (bytes.length > 8) {
            throw new IllegalArgumentException("Cannot convert more than 8 bytes to a long.");
        }

        long result = 0;
        if (isBigEndian()) {
            for (byte aByte : bytes) {
                result <<= 8;
                result |= (aByte & 0xFFL);
            }
        } else {
            for (int i = bytes.length - 1; i >= 0; i--) {
                result <<= 8;
                result |= (bytes[i] & 0xFFL);
            }
        }

        // Sign extension
        int shift = (8 - bytes.length) * 8;
        return (result << shift) >> shift;
    }

    /**
     * Converts the byte array to a BigInteger value.
     *
     * @param bytes the byte array to convert
     * @return the BigInteger value
     */
    public BigInteger toBigInteger(byte[] bytes) {
        byte[] copy = bytes.clone();
        if (!isBigEndian()) {
            reverseInPlace(copy);
        }
        return new BigInteger(copy);
    }

    private void reverseInPlace(byte[] array) {
        for (int i = 0; i < array.length / 2; i++) {
            byte temp = array[i];
            array[i] = array[array.length - 1 - i];
            array[array.length - 1 - i] = temp;
        }
    }

    /**
     * Converts the byte array to a string representation using Long.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the time value
     */
    public String toString(byte[] bytes) {
        return toLong(bytes).toString();
    }

    @Override
    public HdfDataFile getDataFile() {
        return dataFile;
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return List.of();
    }

    /**
     * Returns the datatype class for this time datatype.
     *
     * @return DatatypeClass.TIME, indicating an HDF5 time datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.TIME;
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

    /**
     * Returns the size of the datatype message data.
     *
     * @return the size of the message data in bytes, as a short
     */
    @Override
    public int getSizeMessageData() {
        return 2; // Only bitPrecision
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort((short) bitPrecision);
    }

    /**
     * Sets the global heap for this datatype (no-op for TimeDatatype).
     *
     * @param grok the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap grok) {
    }

    /**
     * Returns a string representation of this TimeDatatype.
     *
     * @return a string describing the datatype's size and bit precision
     */
    @Override
    public String toString() {
        return "TimeDatatype{" +
                "size=" + size +
                ", bitPrecision=" + bitPrecision +
                '}';
    }

    /**
     * Returns the class and version byte for this datatype.
     *
     * @return the class and version byte
     */
    @Override
    public int getClassAndVersion() {
        return classAndVersion;
    }

    /**
     * Returns the size of the time data in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }
}