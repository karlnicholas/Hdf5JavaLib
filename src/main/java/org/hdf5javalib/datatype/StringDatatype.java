package org.hdf5javalib.datatype;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.hdfjava.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Represents an HDF5 String Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code StringDatatype} class models a fixed-length string in HDF5, supporting parsing from a
 * {@link ByteBuffer} and conversion to Java types such as {@link String}, {@link HdfString},
 * or {@code byte[]}. It handles padding and character set configuration as per the HDF5 string
 * datatype (class 3).
 * </p>
 *
 * @see Datatype
 * @see HdfGlobalHeap
 */
public class StringDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 3, version 1).
     */
    private final int classAndVersion;
    /**
     * A BitSet containing class-specific bit field information (padding type and character set).
     */
    private final BitSet classBitField;
    /**
     * The fixed size of the string in bytes.
     */
    private final int size;
    private final HdfDataFile dataFile;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<StringDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfString.class, HdfString::new);
        CONVERTERS.put(HdfData.class, HdfString::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes); // Raw reference bytes
    }

    /**
     * Constructs a StringDatatype representing an HDF5 string datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet containing class-specific bit field information
     * @param size            the fixed size of the string in bytes
     * @param dataFile
     */
    public StringDatatype(int classAndVersion, BitSet classBitField, int size, HdfDataFile dataFile) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.dataFile = dataFile;
    }

    /**
     * Parses an HDF5 string datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet containing class-specific bit field information
     * @param size            the fixed size of the string in bytes
     * @param ignoredBuffer   the ByteBuffer (ignored as string datatype has no properties)
     * @return a new StringDatatype instance
     */
    public static StringDatatype parseStringType(int classAndVersion, BitSet classBitField, int size, ByteBuffer ignoredBuffer, HdfDataFile dataFile) {
        return new StringDatatype(classAndVersion, classBitField, size, dataFile);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 string datatype.
     *
     * @param paddingType the padding type for the string
     * @param charSet     the character set (ASCII or UTF-8)
     * @return a BitSet encoding the padding type and character set
     */
    public static BitSet createClassBitField(PaddingType paddingType, CharacterSet charSet) {
        return BitSet.valueOf(new long[]{((long) charSet.value << 4) + paddingType.value});
    }

    /**
     * Creates a fixed class and version byte for an HDF5 string datatype.
     *
     * @return a byte representing class 3 and version 1, as defined by the HDF5 specification
     */
    @SuppressWarnings("SameReturnValue")
    public static byte createClassAndVersion() {
        return 0x13;
    }

    /**
     * Registers a converter for transforming StringDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between StringDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<StringDatatype, T> converter) {
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
        DatatypeConverter<StringDatatype, T> converter = (DatatypeConverter<StringDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<StringDatatype, ?>> entry : CONVERTERS.entrySet()) {
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
     * @return false, as StringDatatype does not require a global heap
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required | false;
    }

    /**
     * Converts the byte array to a string, handling padding and termination based on the padding type.
     *
     * @param bytes the byte array to convert
     * @return the string representation, decoded using ASCII or UTF-8
     */
    public String toString(byte[] bytes) {
        byte[] workingBytes = getWorkingBytes(bytes);
        int length = size;

        // Handle padding types
        if (getPaddingType() == PaddingType.NULL_TERMINATE || getPaddingType() == PaddingType.NULL_PAD) {
            // Find first NUL byte
            for (int i = 0; i < size; i++) {
                if (workingBytes[i] == 0x00) {
                    length = i;
                    break;
                }
            }
        }
        // SPACE_PAD: Use all bytes, including spaces

        // Copy valid bytes
        byte[] validBytes = new byte[length];
        System.arraycopy(workingBytes, 0, validBytes, 0, length);

        // Decode using character set
        return new String(validBytes,
                getCharacterSet() == CharacterSet.ASCII ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8);
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
     * Prepares a working byte array for string conversion, applying padding based on the padding type.
     *
     * @param bytes the input byte array
     * @return a byte array of the correct size, padded with NULs or spaces if necessary
     */
    public byte[] getWorkingBytes(byte[] bytes) {
        byte[] workingBytes = new byte[size];
        int workingEnd = Math.min(size, bytes.length);
        System.arraycopy(bytes, 0, workingBytes, 0, workingEnd);

        // Apply padding based on padding type
        if (workingEnd < size) {
            if (getPaddingType() == PaddingType.NULL_PAD || getPaddingType() == PaddingType.NULL_TERMINATE) {
                Arrays.fill(workingBytes, workingEnd, size, (byte) 0x00); // NUL padding
            } else if (getPaddingType() == PaddingType.SPACE_PAD) {
                Arrays.fill(workingBytes, workingEnd, size, (byte) ' '); // Space padding
            }
        }
        return workingBytes;
    }

    /**
     * Returns the datatype class for this string datatype.
     *
     * @return DatatypeClass.STRING, indicating an HDF5 string datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.STRING;
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
        return (short) (0 + 8);
    }

    /**
     * Returns a string representation of this StringDatatype.
     *
     * @return a string describing the datatype's size, padding type, and character set
     */
    @Override
    public String toString() {
        return "StringDatatype{" +
                "size=" + size +
                ", padding='" + getPaddingType().name + '\'' +
                ", charSet='" + getCharacterSet().name + '\'' +
                '}';
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
    }

    /**
     * Enum representing padding types for HDF5 strings.
     */
    public enum PaddingType {
        /**
         * Null-terminated string, padded with nulls for longer strings.
         */
        NULL_TERMINATE(0, "Null Terminate",
                "A zero byte marks the end of the string and is guaranteed to be present after converting a long string to a short string. " +
                        "When converting a short string to a long string, the value is padded with additional null characters as necessary."),

        /**
         * Null-padded string, truncated for shorter strings.
         */
        NULL_PAD(1, "Null Pad",
                "Null characters are added to the end of the value during conversions from short values to long values, " +
                        "but conversion in the opposite direction simply truncates the value."),

        /**
         * Space-padded string, truncated for shorter strings (Fortran style).
         */
        SPACE_PAD(2, "Space Pad",
                "Space characters are added to the end of the value during conversions from short values to long values, " +
                        "but conversion in the opposite direction simply truncates the value. This is the Fortran representation of the string."),

        /**
         * Reserved padding type for future use.
         */
        RESERVED_3(3, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_4(4, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_5(5, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_6(6, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_7(7, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_8(8, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_9(9, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_10(10, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_11(11, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_12(12, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_13(13, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_14(14, "Reserved", "Reserved for future use."),
        /**
         * Reserved padding type for future use.
         */
        RESERVED_15(15, "Reserved", "Reserved for future use.");

        private final int value;
        private final String name;
        private final String description;

        PaddingType(int value, String name, String description) {
            this.value = value;
            this.name = name;
            this.description = description;
        }

        /**
         * Retrieves the PaddingType corresponding to the given value.
         *
         * @param value the numeric value of the padding type
         * @return the corresponding PaddingType
         * @throws IllegalArgumentException if the value does not match any known padding type
         */
        public static PaddingType fromValue(int value) {
            for (PaddingType type : values()) {
                if (type.value == value) return type;
            }
            throw new IllegalArgumentException("Invalid padding type value: " + value);
        }

        /**
         * Retrieves the PaddingType from a BitSet.
         *
         * @param bits the BitSet containing padding type information
         * @return the corresponding PaddingType
         */
        public static PaddingType fromBitSet(BitSet bits) {
            int value = (bits.get(0) ? 1 : 0) | (bits.get(1) ? 2 : 0) |
                    (bits.get(2) ? 4 : 0) | (bits.get(3) ? 8 : 0);
            return fromValue(value);
        }
    }

    /**
     * Enum representing character sets for HDF5 strings.
     */
    public enum CharacterSet {
        /**
         * ASCII character set.
         */
        ASCII(0, "ASCII", "American Standard Code for Information Interchange"),
        /**
         * UTF-8 character set.
         */
        UTF8(1, "UTF-8", "Unicode Transformation Format, 8-bit"),
        /**
         * Reserved character set for future use.
         */
        RESERVED_2(2, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_3(3, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_4(4, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_5(5, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_6(6, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_7(7, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_8(8, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_9(9, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_10(10, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_11(11, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_12(12, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_13(13, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_14(14, "Reserved", "Reserved for future use."),
        /**
         * Reserved character set for future use.
         */
        RESERVED_15(15, "Reserved", "Reserved for future use.");

        final int value;
        final String name;
        private final String description;

        CharacterSet(int value, String name, String description) {
            this.value = value;
            this.name = name;
            this.description = description;
        }

        /**
         * Retrieves the CharacterSet corresponding to the given value.
         *
         * @param value the numeric value of the character set
         * @return the corresponding CharacterSet
         * @throws IllegalArgumentException if the value does not match any known character set
         */
        public static CharacterSet fromValue(int value) {
            for (CharacterSet set : values()) {
                if (set.value == value) return set;
            }
            throw new IllegalArgumentException("Invalid character set value: " + value);
        }

        /**
         * Retrieves the CharacterSet from a BitSet.
         *
         * @param bits the BitSet containing character set information
         * @return the corresponding CharacterSet
         */
        public static CharacterSet fromBitSet(BitSet bits) {
            int value = (bits.get(4) ? 1 : 0) | (bits.get(5) ? 2 : 0) |
                    (bits.get(6) ? 4 : 0) | (bits.get(7) ? 8 : 0);
            return fromValue(value);
        }
    }

    /**
     * Gets the padding type for this string datatype.
     *
     * @return the PaddingType (NULL_TERMINATE, NULL_PAD, or SPACE_PAD)
     */
    public PaddingType getPaddingType() {
        return PaddingType.fromBitSet(classBitField);
    }

    /**
     * Gets the character set for this string datatype.
     *
     * @return the CharacterSet (ASCII or UTF8)
     */
    public CharacterSet getCharacterSet() {
        return CharacterSet.fromBitSet(classBitField);
    }

    /**
     * Sets the global heap for this datatype (no-op for StringDatatype).
     *
     * @param grok the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap grok) {
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
     * Returns the fixed size of the string in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }
}
