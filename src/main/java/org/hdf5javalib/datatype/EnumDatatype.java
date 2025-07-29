package org.hdf5javalib.datatype;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfEnum;
import org.hdf5javalib.hdffile.dataobjects.messages.DatatypeMessage;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.hdfjava.HdfDataFile;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Represents an HDF5 Enumerated Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code EnumDatatype} class models an enumerated datatype in HDF5, mapping integer values to string names.
 * It supports parsing from a {@link ByteBuffer}, conversion to Java types such as {@link HdfEnum},
 * {@code String}, or {@code byte[]}, and is defined by a base integer type and a set of name-value pairs,
 * as per the HDF5 enumerated datatype (class 8).
 * </p>
 *
 * @see Datatype
 * @see DatatypeMessage
 */
public class EnumDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 8, version 1 or 2).
     */
    private final int classAndVersion;
    /**
     * A BitSet indicating the number of enumeration members.
     */
    private final BitSet classBitField;
    /**
     * The size of each enumeration value in bytes, matching the base type.
     */
    private final int size;
    /**
     * The base integer datatype for enumeration values.
     */
    private final Datatype datatype;
    /**
     * The array of enumeration names.
     */
    private final String[] names;
    /**
     * The packed array of enumeration values.
     */
    private final byte[] values;
    private final HdfDataFile dataFile;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<EnumDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfEnum.class, HdfEnum::new);
        CONVERTERS.put(HdfData.class, HdfEnum::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    /**
     * Constructs an EnumDatatype representing an HDF5 enumerated datatype.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet indicating the number of members
     * @param size            the size of each enumeration value in bytes
     * @param datatype        the base integer datatype for values
     * @param names           the array of enumeration names
     * @param values          the packed array of enumeration values
     * @param dataFile
     * @throws IllegalArgumentException if the number of names or values does not match the specification
     */
    public EnumDatatype(int classAndVersion, BitSet classBitField, int size,
                        Datatype datatype, String[] names, byte[] values, HdfDataFile dataFile) {
        this.dataFile = dataFile;
        if (names.length != getNumberOfMembers(classBitField)) {
            throw new IllegalArgumentException("Number of names doesn't match classBitField specification");
        }
        if (values.length != names.length * size) {
            throw new IllegalArgumentException("Values array length doesn't match expected size");
        }
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.datatype = datatype;
        this.names = names.clone();
        this.values = values.clone();
    }

    /**
     * Parses an HDF5 enumerated datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet indicating the number of members
     * @param size            the size of each enumeration value in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new EnumDatatype instance parsed from the buffer
     */
    public static EnumDatatype parseEnumDatatype(int classAndVersion, BitSet classBitField,
                                                 int size, ByteBuffer buffer, HdfDataFile hdfDataFile) {
        // Base type is parsed from the buffer first, after size
        Datatype baseType = DatatypeMessage.getHdfDatatype(buffer, hdfDataFile);

        int numMembers = getNumberOfMembers(classBitField);

        // Read names (null-terminated ASCII strings, padded to 8-byte multiples)
        String[] names = new String[numMembers];
        for (int i = 0; i < numMembers; i++) {
            ArrayList<Byte> nameBytesList = new ArrayList<>();
            boolean nullFound = false;
            while (!nullFound) {
                byte[] chunk = new byte[8];
                buffer.get(chunk);
                for (byte b : chunk) {
                    if (b == 0) {
                        nullFound = true;
                        break;
                    }
                    nameBytesList.add(b);
                }
            }
            byte[] nameBytes = new byte[nameBytesList.size()];
            for (int j = 0; j < nameBytes.length; j++) {
                nameBytes[j] = nameBytesList.get(j);
            }
            names[i] = new String(nameBytes, StandardCharsets.US_ASCII);
        }

        // Read values (packed, no padding)
        byte[] values = new byte[numMembers * size];
        buffer.get(values);

        return new EnumDatatype(classAndVersion, classBitField, size, baseType, names, values, hdfDataFile);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 enumerated datatype.
     *
     * @param numberOfMembers the number of enumeration members
     * @return a 24-bit BitSet encoding the number of members
     * @throws IllegalArgumentException if the number of members is not between 0 and 65535
     */
    public static BitSet createClassBitField(int numberOfMembers) {
        if (numberOfMembers < 0 || numberOfMembers > 65535) {
            throw new IllegalArgumentException("Number of members must be between 0 and 65535");
        }
        BitSet bits = new BitSet(24);
        // Set bits 0-15 for number of members
        for (int i = 0; i < 16; i++) {
            bits.set(i, (numberOfMembers & (1 << i)) != 0);
        }
        // Bits 16-23 reserved as zero
        return bits;
    }

    /**
     * Creates a class and version byte for an HDF5 enumerated datatype.
     *
     * @param version the version number (1 or 2)
     * @return a byte representing class 8 and the specified version
     * @throws IllegalArgumentException if the version is not 1 or 2
     */
    public static byte createClassAndVersion(int version) {
        if (version != 1 && version != 2) {
            throw new IllegalArgumentException("Enum Datatype only supports versions 1 and 2");
        }
        return (byte) ((8 << 4) | version); // Class 8, specified version
    }

    /**
     * Retrieves the number of members from the class bit field.
     *
     * @param classBitField the BitSet indicating the number of members
     * @return the number of enumeration members
     */
    public static int getNumberOfMembers(BitSet classBitField) {
        int num = 0;
        for (int i = 0; i < 16; i++) {
            if (classBitField.get(i)) {
                num |= 1 << i;
            }
        }
        return num;
    }

    /**
     * Registers a converter for transforming EnumDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between EnumDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<EnumDatatype, T> converter) {
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
    public <T> T getInstance(Class<T> clazz, byte[] bytes) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        @SuppressWarnings("unchecked")
        DatatypeConverter<EnumDatatype, T> converter = (DatatypeConverter<EnumDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<EnumDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    /**
     * Converts the byte array to the corresponding enumeration name.
     *
     * @param bytes the byte array to convert
     * @return the enumeration name matching the byte value, or "undefined" if no match
     * @throws IllegalArgumentException if the byte array length does not match the datatype size
     */
    public String toString(byte[] bytes) {
        int valueIndex = findValueIndex(bytes);
        return valueIndex >= 0 ? names[valueIndex] : "undefined";
    }

    @Override
    public HdfDataFile getDataFile() {
        return dataFile;
    }

    private int findValueIndex(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length doesn't match datatype size");
        }
        for (int i = 0; i < names.length; i++) {
            byte[] valueBytes = Arrays.copyOfRange(values, i * size, (i + 1) * size);
            if (Arrays.equals(bytes, valueBytes)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the datatype class for this enumerated datatype.
     *
     * @return DatatypeClass.ENUM, indicating an HDF5 enumerated datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.ENUM;
    }

    /**
     * Returns the class bit field for this datatype.
     *
     * @return the BitSet indicating the number of members
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
        int totalNameBytes = 0;
        for (String name : names) {
            int nameLength = name.getBytes(StandardCharsets.US_ASCII).length + 1; // Include null terminator
            totalNameBytes += (nameLength + 7) & ~7; // Round up to next 8-byte multiple
        }
        return (short) (totalNameBytes + values.length + datatype.getSize());
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // Write base type definition first
        datatype.writeDefinitionToByteBuffer(buffer);
        // Write names (null-terminated, padded to 8-byte multiples)
        for (String name : names) {
            byte[] nameBytes = name.getBytes(StandardCharsets.US_ASCII);
            buffer.put(nameBytes);
            buffer.put((byte) 0); // Null terminator
            int padding = 8 - ((nameBytes.length + 1) % 8);
            if (padding < 8) { // If not already on 8-byte boundary
                buffer.put(new byte[padding]);
            }
        }
        // Write values (packed)
        buffer.put(values);
    }

    /**
     * Sets the global heap for this datatype (no-op for EnumDatatype).
     *
     * @param globalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        datatype.setGlobalHeap(globalHeap);
    }

    /**
     * Returns a string representation of this EnumDatatype.
     *
     * @return a string describing the datatype's size, number of members, and base type
     */
    @Override
    public String toString() {
        return "EnumDatatype{" +
                "size=" + size +
                ", numMembers=" + names.length +
                ", datatype=" + datatype +
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
     * Returns the size of each enumeration value in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return datatype.getReferenceInstances();
    }

}