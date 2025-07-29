package org.hdf5javalib.datatype;

import org.hdf5javalib.dataclass.HdfBitField;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.hdfjava.HdfDataFile;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents an HDF5 Bitfield Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code BitFieldDatatype} class models a bitfield in HDF5, supporting parsing from a {@link ByteBuffer},
 * conversion to Java types such as {@link BitSet}, {@code String}, or {@link HdfBitField}, and configuration
 * of byte order and padding as per the HDF5 bitfield datatype (class 4).
 * </p>
 *
 * @see Datatype
 * @see HdfGlobalHeap
 */
public class BitFieldDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 4, version 1).
     */
    private final int classAndVersion;
    /**
     * A BitSet containing class-specific bit field information (byte order and padding).
     */
    private final BitSet classBitField;
    /**
     * The total size of the bitfield datatype in bytes.
     */
    private final int size;
    /**
     * The bit offset of the first significant bit.
     */
    private final int bitOffset;
    /**
     * The number of bits of precision.
     */
    private final int bitPrecision;
    private final HdfDataFile hdfDataFile;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<BitFieldDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(BitSet.class, (bytes, dt) -> dt.toBitSet(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfBitField.class, HdfBitField::new);
        CONVERTERS.put(HdfData.class, HdfBitField::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    /**
     * Constructs a BitFieldDatatype representing an HDF5 bitfield datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet containing class-specific bit field information
     * @param size            the total size of the bitfield datatype in bytes
     * @param bitOffset       the bit offset of the first significant bit
     * @param bitPrecision    the number of bits of precision
     * @param hdfDataFile
     */
    public BitFieldDatatype(int classAndVersion, BitSet classBitField, int size, int bitOffset, int bitPrecision, HdfDataFile hdfDataFile) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
        this.hdfDataFile = hdfDataFile;
    }

    /**
     * Parses an HDF5 bitfield datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet containing class-specific bit field information
     * @param size            the total size of the bitfield datatype in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new BitFieldDatatype instance parsed from the buffer
     */
    public static BitFieldDatatype parseBitFieldType(int classAndVersion, BitSet classBitField, int size, ByteBuffer buffer, HdfDataFile hdfDataFile) {
        int bitOffset = Short.toUnsignedInt(buffer.getShort());
        int bitPrecision = Short.toUnsignedInt(buffer.getShort());
        return new BitFieldDatatype(classAndVersion, classBitField, size, bitOffset, bitPrecision, hdfDataFile);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 bitfield datatype.
     *
     * @param bigEndian  true for big-endian byte order, false for little-endian
     * @param loPadValue the low padding value (0 or 1)
     * @param hiPadValue the high padding value (0 or 1)
     * @return a 24-bit BitSet with byte order and padding settings
     * @throws IllegalArgumentException if loPadValue or hiPadValue is not 0 or 1
     */
    public static BitSet createClassBitField(boolean bigEndian, int loPadValue, int hiPadValue) {
        if (loPadValue != 0 && loPadValue != 1) throw new IllegalArgumentException("loPadValue must be 0 or 1");
        if (hiPadValue != 0 && hiPadValue != 1) throw new IllegalArgumentException("hiPadValue must be 0 or 1");
        BitSet bits = new BitSet(24);
        bits.set(0, bigEndian);     // Bit 0: Byte Order (0 = little-endian, 1 = big-endian)
        bits.set(1, loPadValue == 1); // Bit 1: Low padding value (0 or 1)
        bits.set(2, hiPadValue == 1); // Bit 2: High padding value (0 or 1)
        // Bits 3-23 reserved as zero
        return bits;
    }

    /**
     * Creates a fixed class and version byte for an HDF5 bitfield datatype.
     *
     * @return a byte representing class 4 and version 1, as defined by the HDF5 specification
     */
    public static byte createClassAndVersion() {
        return 0x14; // Version 1, Class 4 for BitField
    }

    /**
     * Registers a converter for transforming BitFieldDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between BitFieldDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<BitFieldDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    /**
     * Converts byte data to an instance of the specified class using registered converters.
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @param bytes the byte array containing the data
     * @return an instance of type T created from the byte array
     * @throws UnsupportedOperationException if no suitable converter is found for the specified class
     */
    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        @SuppressWarnings("unchecked")
        DatatypeConverter<BitFieldDatatype, T> converter = (DatatypeConverter<BitFieldDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<BitFieldDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    /**
     * Checks if the bitfield uses big-endian byte order.
     *
     * @return true if big-endian, false if little-endian
     */
    public boolean isBigEndian() {
        return classBitField.get(0);
    }

    /**
     * Returns the low padding value for the bitfield.
     *
     * @return the low padding value (0 or 1)
     */
    public int getLoPadValue() {
        return classBitField.get(1) ? 1 : 0; // Return the padding value (0 or 1)
    }

    /**
     * Returns the high padding value for the bitfield.
     *
     * @return the high padding value (0 or 1)
     */
    public int getHiPadValue() {
        return classBitField.get(2) ? 1 : 0; // Return the padding value (0 or 1)
    }

    /**
     * Converts the byte array to a BitSet representing the bitfield's significant bits.
     *
     * @param bytes the byte array to convert
     * @return a BitSet containing the significant bits with padding applied
     * @throws IllegalArgumentException if the byte array length does not match the datatype size
     */
    public BitSet toBitSet(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length + ") does not match datatype size (" + size + ")");
        }

        // Convert bytes to BitSet with correct byte order
        BitSet fullBitSet = BitSet.valueOf(isBigEndian() ? bytes : reverseBytes(bytes));
        int totalBits = size * 8;

        // Apply padding if bitPrecision < totalBits
        if (bitPrecision < totalBits) {
            int loPadValue = getLoPadValue();
            for (int i = 0; i < bitOffset; i++) {
                fullBitSet.set(i, loPadValue == 1);
            }
            int hiPadValue = getHiPadValue();
            for (int i = bitOffset + bitPrecision; i < totalBits; i++) {
                fullBitSet.set(i, hiPadValue == 1);
            }
        }

        // Extract the significant bits
        BitSet result = new BitSet(bitPrecision);
        for (int i = 0; i < bitPrecision; i++) {
            if (fullBitSet.get(bitOffset + i)) {
                result.set(i);
            }
        }
        return result;
    }

    private byte[] reverseBytes(byte[] array) {
        byte[] reversed = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            reversed[i] = array[array.length - 1 - i];
        }
        return reversed;
    }

    /**
     * Converts the byte array to a string representation of the bitfield's bits.
     *
     * @param bytes the byte array to convert
     * @return a string of '0' and '1' characters representing the bitfield
     */
    public String toString(byte[] bytes) {
        BitSet bitSet = toBitSet(bytes);
        StringBuilder sb = new StringBuilder();
        for (int i = bitPrecision - 1; i >= 0; i--) {
            sb.append(bitSet.get(i) ? "1" : "0");
        }
        return sb.toString();
    }

    @Override
    public HdfDataFile getDataFile() {
        return hdfDataFile;
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return List.of();
    }

    /**
     * Returns the datatype class for this bitfield datatype.
     *
     * @return DatatypeClass.BITFIELD, indicating an HDF5 bitfield datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.BITFIELD;
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
        return 4; // 2 bytes for bitOffset + 2 bytes for bitPrecision
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort((short) bitOffset);
        buffer.putShort((short) bitPrecision);
    }

    /**
     * Sets the global heap for this datatype (no-op for BitFieldDatatype).
     *
     * @param ignoreGlobalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap ignoreGlobalHeap) {
        // no global heap needed, ignored
    }

    /**
     * Returns a string representation of this BitFieldDatatype.
     *
     * @return a string describing the datatype's size, bit offset, precision, byte order, and padding
     */
    @Override
    public String toString() {
        return "BitFieldDatatype{" +
                "size=" + size +
                ", bitOffset=" + bitOffset +
                ", bitPrecision=" + bitPrecision +
                ", bigEndian=" + isBigEndian() +
                ", loPadValue=" + getLoPadValue() +
                ", hiPadValue=" + getHiPadValue() +
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
     * Returns the total size of the bitfield datatype in bytes.
     *
     * @return the total size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }

    public int getBitOffset() {
        return bitOffset;
    }

    public int getBitPrecision() {
        return bitPrecision;
    }
}