package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfReference;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an HDF5 Reference Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code ReferenceDatatype} class models a reference datatype in HDF5, which points to objects
 * or dataset regions. It supports parsing from a {@link java.nio.ByteBuffer} and conversion to Java
 * types such as {@link HdfReference}, {@code String}, or {@code byte[]}, as per the HDF5 reference
 * datatype (class 7).
 * </p>
 *
 * @see org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype
 * @see org.hdf5javalib.file.infrastructure.HdfGlobalHeap
 */
@Getter
public class ReferenceDatatype implements HdfDatatype {
    /** The class and version information for the datatype (class 7, version 1). */
    private final byte classAndVersion;
    /** A BitSet indicating the reference type. */
    private final BitSet classBitField;
    /** The size of the reference data in bytes (e.g., 8 for object reference). */
    private final int size;

    /**
     * Enum representing the types of HDF5 references.
     */
    public enum ReferenceType {
        /**
         * Reference to an HDF5 object.
         */
        OBJECT_REFERENCE(0, "Object Reference"),
        /**
         * Reference to a dataset region.
         */
        DATASET_REGION_REFERENCE(1, "Dataset Region Reference");

        private final int value;
        private final String description;

        ReferenceType(int value, String description) {
            this.value = value;
            this.description = description;
        }

        /**
         * Gets the numeric value of the reference type.
         *
         * @return the numeric value
         */
        public int getValue() {
            return value;
        }

        /**
         * Retrieves the ReferenceType corresponding to the given value.
         *
         * @param value the numeric value of the reference type
         * @return the corresponding ReferenceType
         * @throws IllegalArgumentException if the value does not match any known reference type
         */
        public static ReferenceType fromValue(int value) {
            for (ReferenceType type : values()) {
                if (type.value == value) return type;
            }
            throw new IllegalArgumentException("Unknown reference type value: " + value);
        }
    }

    /** Map of converters for transforming byte data to specific Java types. */
    private static final Map<Class<?>, HdfConverter<ReferenceDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfReference.class, HdfReference::new);
        CONVERTERS.put(HdfData.class, HdfReference::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes); // Raw reference bytes
    }

    /**
     * Constructs a ReferenceDatatype representing an HDF5 reference datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet indicating the reference type
     * @param size            the size of the reference data in bytes
     * @throws IllegalArgumentException if the reference type is invalid or reserved bits are non-zero
     */
    public ReferenceDatatype(byte classAndVersion, BitSet classBitField, int size) {
        int typeValue = getTypeValue(classBitField);
        if (typeValue > 1) { // Only 0 and 1 are defined
            throw new IllegalArgumentException("Invalid reference type value: " + typeValue);
        }
        // Check reserved bits (4-23) are zero
        for (int i = 4; i < 24; i++) {
            if (classBitField.get(i)) {
                throw new IllegalArgumentException("Reserved bits (4-23) must be zero in Reference classBitField");
            }
        }
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
    }

    /**
     * Parses an HDF5 reference datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet indicating the reference type
     * @param size            the size of the reference data in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new ReferenceDatatype instance
     */
    public static ReferenceDatatype parseReferenceDatatype(byte classAndVersion, BitSet classBitField,
                                                           int size, ByteBuffer buffer) {
        // No properties to parse for Reference datatype
        return new ReferenceDatatype(classAndVersion, classBitField, size);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 reference datatype.
     *
     * @param type the reference type (OBJECT_REFERENCE or DATASET_REGION_REFERENCE)
     * @return a 24-bit BitSet encoding the reference type
     */
    public static BitSet createClassBitField(ReferenceType type) {
        BitSet bits = new BitSet(24);
        int typeValue = type.getValue();
        for (int i = 0; i < 4; i++) {
            bits.set(i, (typeValue & (1 << i)) != 0);
        }
        // Bits 4-23 remain zero (reserved)
        return bits;
    }

    /**
     * Creates a fixed class and version byte for an HDF5 reference datatype.
     *
     * @return a byte representing class 7 and version 1, as defined by the HDF5 specification
     */
    public static byte createClassAndVersion() {
        return (byte) ((7 << 4) | 1); // Class 7, version 1 (no version 2 defined)
    }

    /**
     * Retrieves the reference type value from the class bit field.
     *
     * @param classBitField the BitSet indicating the reference type
     * @return the numeric value of the reference type
     */
    public static int getTypeValue(BitSet classBitField) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            if (classBitField.get(i)) {
                value |= 1 << i;
            }
        }
        return value;
    }

    /**
     * Gets the reference type of this datatype.
     *
     * @return the ReferenceType (OBJECT_REFERENCE or DATASET_REGION_REFERENCE)
     */
    public ReferenceType getReferenceType() {
        return ReferenceType.fromValue(getTypeValue(classBitField));
    }

    /**
     * Registers a converter for transforming ReferenceDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the HdfConverter for converting between ReferenceDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, HdfConverter<ReferenceDatatype, T> converter) {
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
        HdfConverter<ReferenceDatatype, T> converter = (HdfConverter<ReferenceDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<ReferenceDatatype, ?>> entry : CONVERTERS.entrySet()) {
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
     * @return false, as ReferenceDatatype does not require a global heap
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required | false;
    }

    /**
     * Converts the byte array to a hexadecimal string representation with reference type.
     *
     * @param bytes the byte array to convert
     * @return a string with the reference type and hexadecimal representation of the bytes
     * @throws IllegalArgumentException if the byte array length does not match the datatype size
     */
    public String toString(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return "Reference[" + getReferenceType().description + "]=" + sb.toString();
    }

    /**
     * Returns the datatype class for this reference datatype.
     *
     * @return DatatypeClass.REFERENCE, indicating an HDF5 reference datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.REFERENCE;
    }

    /**
     * Returns the class bit field for this datatype.
     *
     * @return the BitSet indicating the reference type
     */
    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    /**
     * Returns the size of the datatype message data.
     *
     * @return zero, as ReferenceDatatype has no properties in the datatype message
     */
    @Override
    public short getSizeMessageData() {
        return 0; // No properties in the datatype message
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // No properties to write
    }

    /**
     * Sets the global heap for this datatype (no-op for ReferenceDatatype).
     *
     * @param globalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        // Empty implementation to satisfy interface
    }

    /**
     * Returns a string representation of this ReferenceDatatype.
     *
     * @return a string describing the datatype's size and reference type
     */
    @Override
    public String toString() {
        return "ReferenceDatatype{" +
                "size=" + size +
                ", type=" + getReferenceType().description +
                '}';
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
     * Returns the size of the reference data in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }
}