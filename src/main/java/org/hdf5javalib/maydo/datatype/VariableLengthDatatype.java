package org.hdf5javalib.maydo.datatype;

import org.hdf5javalib.maydo.dataclass.HdfData;
import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.dataclass.HdfVariableLength;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DatatypeMessage;
import org.hdf5javalib.maydo.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Represents an HDF5 Variable-Length Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code VariableLengthDatatype} class models a variable-length sequence or string in HDF5,
 * supporting parsing from a {@link ByteBuffer} and conversion to Java types such as
 * {@link String}, {@link HdfVariableLength}, or arrays. It requires a global heap for data storage
 * and handles type, padding, and character set configuration as per the HDF5 variable-length
 * datatype (class 9).
 * </p>
 *
 * @see Datatype
 * @see DatatypeMessage
 * @see HdfGlobalHeap
 */
public class VariableLengthDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 9, version 1).
     */
    private final int classAndVersion;
    /**
     * A BitSet containing class-specific bit field information (type, padding, character set).
     */
    private final BitSet classBitField;
    /**
     * The fixed size of the variable-length descriptor in bytes.
     */
    private final int size;
    /**
     * The global heap for accessing variable-length data.
     */
    private HdfGlobalHeap globalHeap;
    /**
     * The underlying datatype for the variable-length elements.
     */
    private final Datatype datatype;
    private final HdfDataFile hdfDataFile;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<VariableLengthDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfVariableLength.class, HdfVariableLength::new);
        CONVERTERS.put(HdfData.class, HdfVariableLength::new);
        CONVERTERS.put(Object.class, (bytes, dt) -> dt.toObjectArray(bytes));
        CONVERTERS.put(byte[][].class, (bytes, dt) -> dt.toByteArrayArray(bytes)); // Raw reference bytes
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    /**
     * Constructs a VariableLengthDatatype representing an HDF5 variable-length datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet containing class-specific bit field information
     * @param size            the fixed size of the variable-length descriptor in bytes
     * @param datatype     the underlying datatype for the variable-length elements
     */
    public VariableLengthDatatype(int classAndVersion, BitSet classBitField, int size, Datatype datatype, HdfDataFile hdfDataFile) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.datatype = datatype;
        this.hdfDataFile = hdfDataFile;
    }

    /**
     * Parses an HDF5 variable-length datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet containing class-specific bit field information
     * @param size            the fixed size of the variable-length descriptor in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new VariableLengthDatatype instance parsed from the buffer
     */
    public static VariableLengthDatatype parseVariableLengthDatatype(int classAndVersion, BitSet classBitField, int size, ByteBuffer buffer, HdfDataFile hdfDataFile) {
        return new VariableLengthDatatype(classAndVersion, classBitField, size, DatatypeMessage.getHdfDatatype(buffer, hdfDataFile), hdfDataFile);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 variable-length datatype.
     *
     * @param type        the variable-length type (SEQUENCE or STRING)
     * @param paddingType the padding type for strings
     * @param charSet     the character set for strings (ASCII or UTF-8)
     * @return a BitSet encoding the type, padding, and character set
     */
    public static BitSet createClassBitField(Type type, PaddingType paddingType, CharacterSet charSet) {
        long bitfield = ((long) charSet.value << 8) | ((long) paddingType.value << 4) | type.value;
        return BitSet.valueOf(new long[]{bitfield});
    }

    /**
     * Creates a fixed class and version byte for an HDF5 variable-length datatype.
     *
     * @return a byte representing class 9 and version 1, as defined by the HDF5 specification
     */
    @SuppressWarnings("SameReturnValue")
    public static byte createClassAndVersion() {
        return 0x19;
    }

    /**
     * Registers a converter for transforming VariableLengthDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between VariableLengthDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<VariableLengthDatatype, T> converter) {
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
        DatatypeConverter<VariableLengthDatatype, T> converter = (DatatypeConverter<VariableLengthDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<VariableLengthDatatype, ?>> entry : CONVERTERS.entrySet()) {
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
     * @return true, as VariableLengthDatatype requires a global heap
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required | true;
    }

    /**
     * Converts the byte array to a string representation, handling strings or sequences.
     *
     * @param bytes the byte array containing the variable-length descriptor
     * @return a string representation of the data, either a single string or an array of values
     */
    public String toString(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buffer.getInt();
        HdfFixedPoint offset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        int index = buffer.getInt();

        byte[] workingBytes = globalHeap.getDataBytes(offset, index);
        if (getType() == Type.STRING) {
            return new String(workingBytes, getCharacterSet() == CharacterSet.ASCII ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8);
        } else {
            int datatypeSize = datatype.getSize();
            String[] resultArray = new String[count];
            for (int i = 0; i < count; i++) {
                resultArray[i] = datatype.getInstance(String.class, Arrays.copyOfRange(workingBytes, i * datatypeSize, (i + 1) * datatypeSize));
            }
            return Arrays.toString(resultArray);
        }
    }

    @Override
    public HdfDataFile getDataFile() {
        return hdfDataFile;
    }

    private byte[][] toByteArrayArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buffer.getInt();
        HdfFixedPoint offset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        int index = buffer.getInt();

        byte[] workingBytes = globalHeap.getDataBytes(offset, index);
        int datatypeSize = datatype.getSize();
        byte[][] resultArray = new byte[count][];
        for (int i = 0; i < count; i++) {
            resultArray[i] = Arrays.copyOfRange(workingBytes, i * datatypeSize, (i + 1) * datatypeSize);
        }
        return resultArray;
    }

    private Object toObjectArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buffer.getInt();
        HdfFixedPoint offset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        int index = buffer.getInt();

        byte[] workingBytes = globalHeap.getDataBytes(offset, index);
        int datatypeSize = datatype.getSize();
        HdfData[] result = new HdfData[count];
        for (int i = 0; i < count; i++) {
            result[i] = datatype.getInstance(HdfData.class, Arrays.copyOfRange(workingBytes, i * datatypeSize, (i + 1) * datatypeSize));
        }
        return result;
    }

    /**
     * Returns the datatype class for this variable-length datatype.
     *
     * @return DatatypeClass.VLEN, indicating an HDF5 variable-length datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.VLEN;
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
        short sizeMessageData = 16;
        if (datatype.getDatatypeClass() == DatatypeClass.FIXED) {
            sizeMessageData += 4;
        }
        return sizeMessageData;
    }

    /**
     * Returns a string representation of this VariableLengthDatatype.
     *
     * @return a string describing the datatype's size, type, padding, and character set
     */
    @Override
    public String toString() {
        return "VariableLengthDatatype{" +
                "size=" + size +
                ", type='" + getType().name + '\'' +
                ( getType() == Type.STRING ? (
                    ", padding='" + getPaddingType().name + '\'' +
                    ", charSet='" + getCharacterSet().name + '\'')
                : "" ) +
                ", datatype='" + datatype + '\'' +
                '}';
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        DatatypeMessage.writeDatatypeProperties(buffer, datatype);
    }

    /**
     * Gets the variable-length type for this datatype.
     *
     * @return the Type (SEQUENCE or STRING)
     */
    public Type getType() {
        return Type.fromBitSet(classBitField);
    }

    /**
     * Gets the padding type for this datatype.
     *
     * @return the PaddingType (NULL_TERMINATE, NULL_PAD, or SPACE_PAD)
     */
    public PaddingType getPaddingType() {
        return PaddingType.fromBitSet(classBitField);
    }

    /**
     * Gets the character set for this datatype.
     *
     * @return the CharacterSet (ASCII or UTF8)
     */
    public CharacterSet getCharacterSet() {
        return CharacterSet.fromBitSet(classBitField);
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
     * Returns the fixed size of the variable-length descriptor in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        this.globalHeap = globalHeap;
        this.datatype.setGlobalHeap(globalHeap);
    }

    public HdfGlobalHeap getGlobalHeap() {
        return globalHeap;
    }

    /**
     * Enum representing variable-length types for HDF5 variable-length datatypes.
     */
    public enum Type {
        /**
         * A variable-length sequence of any datatype.
         */
        SEQUENCE(0, "Sequence", "A variable-length sequence of any datatype. Variable-length sequences do not have padding or character set information."),

        /**
         * A variable-length string.
         */
        STRING(1, "String", "Unicode Transformation Format, 8-bit"),

        /**
         * Reserved type for future use.
         */
        RESERVED_2(2, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_3(3, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_4(4, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_5(5, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_6(6, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_7(7, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_8(8, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_9(9, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_10(10, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_11(11, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_12(12, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_13(13, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_14(14, "Reserved", "Reserved for future use."),
        /**
         * Reserved type for future use.
         */
        RESERVED_15(15, "Reserved", "Reserved for future use.");

        private final int value;
        private final String name;
        private final String description;

        Type(int value, String name, String description) {
            this.value = value;
            this.name = name;
            this.description = description;
        }

        /**
         * Retrieves the Type corresponding to the given value.
         *
         * @param value the numeric value of the type
         * @return the corresponding Type
         * @throws IllegalArgumentException if the value does not match any known type
         */
        public static Type fromValue(int value) {
            for (Type set : values()) {
                if (set.value == value) return set;
            }
            throw new IllegalArgumentException("Invalid type value: " + value);
        }

        /**
         * Retrieves the Type from a BitSet.
         *
         * @param bits the BitSet containing type information
         * @return the corresponding Type
         */
        public static Type fromBitSet(BitSet bits) {
            int value = (bits.get(0) ? 1 : 0) | (bits.get(1) ? 2 : 0) |
                    (bits.get(2) ? 4 : 0) | (bits.get(3) ? 8 : 0);
            return fromValue(value);
        }
    }

    /**
     * Enum representing padding types for HDF5 variable-length strings.
     */
    public enum PaddingType {
        /**
         * Null-terminated string.
         */
        NULL_TERMINATE(0, "Null Terminate", "A zero byte marks the end of the string..."),

        /**
         * Null-padded string.
         */
        NULL_PAD(1, "Null Pad", "Null characters are added to the end..."),

        /**
         * Space-padded string.
         */
        SPACE_PAD(2, "Space Pad", "Space characters are added to the end..."),

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
            int value = (bits.get(4) ? 1 : 0) | (bits.get(5) ? 2 : 0) |
                    (bits.get(6) ? 4 : 0) | (bits.get(7) ? 8 : 0);
            return fromValue(value);
        }
    }

    /**
     * Enum representing character sets for HDF5 variable-length strings.
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

        private final int value;
        private final String name;
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
            int value = (bits.get(8) ? 1 : 0) | (bits.get(9) ? 2 : 0) |
                    (bits.get(10) ? 4 : 0) | (bits.get(11) ? 8 : 0);
            return fromValue(value);
        }
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return datatype.getReferenceInstances();
    }
}