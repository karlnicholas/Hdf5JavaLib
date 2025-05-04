package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfOpaque;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an HDF5 Opaque Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code OpaqueDatatype} class models an opaque datatype in HDF5, which is a fixed-size block
 * of uninterpreted data with an associated ASCII tag. It supports parsing from a
 * {@link java.nio.ByteBuffer} and conversion to Java types such as {@link HdfOpaque},
 * {@code String}, or {@code byte[]}, as per the HDF5 opaque datatype (class 5).
 * </p>
 *
 * @see org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype
 * @see org.hdf5javalib.file.infrastructure.HdfGlobalHeap
 */
@Getter
public class OpaqueDatatype implements HdfDatatype {
    /** The class and version information for the datatype (class 5, version 1). */
    private final byte classAndVersion;
    /** A BitSet indicating the length of the ASCII tag. */
    private final BitSet classBitField;
    /** The size of the opaque data in bytes. */
    private final int size;
    /** The NUL-terminated ASCII tag, padded to an 8-byte multiple. */
    private final String asciiTag;

    /** Map of converters for transforming byte data to specific Java types. */
    private static final Map<Class<?>, HdfConverter<OpaqueDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfOpaque.class, HdfOpaque::new);
        CONVERTERS.put(HdfData.class, HdfOpaque::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes); // Raw bytes access
    }

    /**
     * Constructs an OpaqueDatatype representing an HDF5 opaque datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet indicating the length of the ASCII tag
     * @param size            the size of the opaque data in bytes
     * @param asciiTag        the NUL-terminated ASCII tag
     * @throws IllegalArgumentException if the ASCII tag length exceeds the bit field specification
     */
    public OpaqueDatatype(byte classAndVersion, BitSet classBitField, int size, String asciiTag) {
        int tagLength = getTagLength(classBitField); // Length including NUL, per bit field
        int actualLength = asciiTag.getBytes(StandardCharsets.US_ASCII).length + 1; // String length + NUL
        if (actualLength > tagLength) {
            throw new IllegalArgumentException("ASCII tag length including NUL (" + actualLength +
                    ") exceeds bit field specification (" + tagLength + ")");
        }
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.asciiTag = asciiTag;
    }

    /**
     * Parses an HDF5 opaque datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet indicating the length of the ASCII tag
     * @param size            the size of the opaque data in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new OpaqueDatatype instance parsed from the buffer
     * @throws IllegalArgumentException if the ASCII tag is not NUL-terminated within the specified length
     */
    public static OpaqueDatatype parseOpaqueDatatype(byte classAndVersion, BitSet classBitField,
                                                     int size, ByteBuffer buffer) {
        int tagLength = getTagLength(classBitField); // Length including NUL, before padding
        int paddedLength = (tagLength + 7) & ~7;     // Round up to next 8-byte multiple
        byte[] tagBytes = new byte[paddedLength];
        buffer.get(tagBytes);

        // Find NUL terminator and extract string
        int nullIndex = 0;
        while (nullIndex < tagLength && tagBytes[nullIndex] != 0) nullIndex++;
        if (nullIndex >= tagLength) {
            throw new IllegalArgumentException("ASCII tag is not NUL-terminated within specified length");
        }
        String asciiTag = new String(tagBytes, 0, nullIndex, StandardCharsets.US_ASCII);

        return new OpaqueDatatype(classAndVersion, classBitField, size, asciiTag);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 opaque datatype.
     *
     * @param tagLength the length of the ASCII tag including NUL terminator
     * @return a 24-bit BitSet encoding the tag length
     * @throws IllegalArgumentException if the tag length is not between 1 and 256 bytes
     */
    public static BitSet createClassBitField(int tagLength) {
        if (tagLength < 1 || tagLength > 256) { // Includes NUL, so min 1 byte
            throw new IllegalArgumentException("Tag length must be between 1 and 256 bytes");
        }
        BitSet bits = new BitSet(24);
        // Set bits 0-7 for tag length
        for (int i = 0; i < 8; i++) {
            bits.set(i, (tagLength & (1 << i)) != 0);
        }
        // Bits 8-23 reserved as zero
        return bits;
    }

    /**
     * Creates a class and version byte for an HDF5 opaque datatype.
     *
     * @param version the version number (must be 1)
     * @return a byte representing class 5 and the specified version
     * @throws IllegalArgumentException if the version is not 1
     */
    public static byte createClassAndVersion(int version) {
        if (version != 1) { // Opaque typically uses version 1
            throw new IllegalArgumentException("Opaque Datatype only supports version 1");
        }
        return (byte) ((5 << 4) | version); // Class 5, specified version
    }

    /**
     * Retrieves the ASCII tag length from the class bit field.
     *
     * @param classBitField the BitSet indicating the tag length
     * @return the length of the ASCII tag including NUL terminator
     */
    public static int getTagLength(BitSet classBitField) {
        int length = 0;
        for (int i = 0; i < 8; i++) {
            if (classBitField.get(i)) {
                length |= 1 << i;
            }
        }
        return length;
    }

    /**
     * Registers a converter for transforming OpaqueDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the HdfConverter for converting between OpaqueDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, HdfConverter<OpaqueDatatype, T> converter) {
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
        HdfConverter<OpaqueDatatype, T> converter = (HdfConverter<OpaqueDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<OpaqueDatatype, ?>> entry : CONVERTERS.entrySet()) {
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
     * @return false, as OpaqueDatatype does not require a global heap
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required | false;
    }

    /**
     * Converts the byte array to a hexadecimal string representation.
     *
     * @param bytes the byte array to convert
     * @return a hexadecimal string of up to 15 bytes, truncated with "..." if longer
     * @throws IllegalArgumentException if the byte array length does not match the datatype size
     */
    public String toString(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        // Return first 15 bytes as hex, truncate if longer
        StringBuilder sb = new StringBuilder();
        int limit = Math.min(bytes.length, 15);
        for (int i = 0; i < limit; i++) {
            sb.append(String.format("%02X", bytes[i]));
        }
        if (bytes.length > 15) {
            sb.append("... (truncated)");
        }
        return sb.toString();
    }

    /**
     * Returns the datatype class for this opaque datatype.
     *
     * @return DatatypeClass.OPAQUE, indicating an HDF5 opaque datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.OPAQUE;
    }

    /**
     * Returns the class bit field for this datatype.
     *
     * @return the BitSet indicating the length of the ASCII tag
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
    public short getSizeMessageData() {
        int tagLength = asciiTag.getBytes(StandardCharsets.US_ASCII).length + 1; // Include NUL
        return (short) ((tagLength + 7) & ~7); // Padded to 8-byte multiple
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        byte[] tagBytes = asciiTag.getBytes(StandardCharsets.US_ASCII);
        buffer.put(tagBytes);
        buffer.put((byte) 0); // NUL terminator
        int padding = 8 - ((tagBytes.length + 1) % 8);
        if (padding < 8) { // If not already on 8-byte boundary
            buffer.put(new byte[padding]);
        }
    }

    /**
     * Sets the global heap for this datatype (no-op for OpaqueDatatype).
     *
     * @param globalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        // Empty implementation to satisfy interface
    }

    /**
     * Returns a string representation of this OpaqueDatatype.
     *
     * @return a string describing the datatype's size and ASCII tag
     */
    @Override
    public String toString() {
        return "OpaqueDatatype{" +
                "size=" + size +
                ", asciiTag='" + asciiTag + '\'' +
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
     * Returns the size of the opaque data in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }
}