package org.hdf5javalib.file.dataobject.message.datatype;

import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents an HDF5 Compound Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code CompoundDatatype} class models a compound datatype in HDF5, consisting of multiple member datatypes.
 * It supports parsing from a {@link java.nio.ByteBuffer}, conversion to Java types such as {@link HdfCompound},
 * {@code HdfData[]}, or {@code String}, and mapping to POJOs using registered converters or reflection, as per
 * the HDF5 compound datatype (class 6).
 * </p>
 *
 * @see org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype
 * @see org.hdf5javalib.file.infrastructure.HdfGlobalHeap
 * @see org.hdf5javalib.file.dataobject.message.DatatypeMessage
 */
public class CompoundDatatype implements HdfDatatype {
    /** The class and version information for the datatype (class 6, version 1). */
    private final byte classAndVersion;
    /** A BitSet indicating the number of members in the compound datatype. */
    private final BitSet classBitField;
    /** The total size of the compound datatype in bytes. */
    private final int size;
    /** The list of member datatypes defining the compound structure. */
    private List<CompoundMemberDatatype> members;

    /** Map of converters for transforming byte data to specific Java types. */
    private static final Map<Class<?>, HdfConverter<CompoundDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfCompound.class, HdfCompound::new);
        CONVERTERS.put(HdfData.class, HdfCompound::new);
        CONVERTERS.put(byte[][].class, (bytes, dt) -> dt.toByteArrayArray(bytes));
        CONVERTERS.put(HdfData[].class, (bytes, dt) -> dt.toHdfDataArray(bytes));
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    /**
     * Constructs a CompoundDatatype with specified members.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet indicating the number of members
     * @param size            the total size of the compound datatype in bytes
     * @param members         the list of member datatypes
     */
    public CompoundDatatype(byte classAndVersion, BitSet classBitField, int size, List<CompoundMemberDatatype> members) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.members = new ArrayList<>(members); // Deep copy to avoid external modification
    }

    /**
     * Constructs a CompoundDatatype by parsing from a ByteBuffer.
     *
     * @param classAndVersion the class and version information for the datatype
     * @param classBitField   a BitSet indicating the number of members
     * @param size            the total size of the compound datatype in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     */
    public CompoundDatatype(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        readFromByteBuffer(buffer);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 compound datatype.
     *
     * @param numberOfMembers the number of members in the compound datatype
     * @return a 16-bit BitSet encoding the number of members
     */
    public static BitSet createClassBitField(short numberOfMembers) {
        // Create a BitSet with capacity for 16 bits (0-15)
        BitSet bitSet = new BitSet(16);

        // Treat the short as a 16-bit pattern and set corresponding bits
        for (int i = 0; i < 16; i++) {
            if ((numberOfMembers & (1 << i)) != 0) {
                bitSet.set(i);
            }
        }

        return bitSet;
    }

    /**
     * Creates a fixed class and version byte for an HDF5 compound datatype.
     *
     * @return a byte representing class 6 and version 1, as defined by the HDF5 specification
     */
    @SuppressWarnings("SameReturnValue")
    public static byte createClassAndVersion() {
        return 0x16;
    }

    private short extractNumberOfMembersFromBitSet() {
        short value = 0;
        for (int i = 0; i < classBitField.length(); i++) {
            if (classBitField.get(i)) {
                value |= (short) (1 << i);
            }
        }
        return value;
    }

    private void readFromByteBuffer(ByteBuffer buffer) {
        this.members = new ArrayList<>();
        int numberOfMembers = extractNumberOfMembersFromBitSet();
        for (int i = 0; i < numberOfMembers; i++) {
            buffer.mark();
            String name = readNullTerminatedString(buffer);

            // Align to 8-byte boundary
            alignBufferTo8ByteBoundary(buffer, name.length() + 1);

            int offset = buffer.getInt();
            int dimensionality = Byte.toUnsignedInt(buffer.get());
            buffer.position(buffer.position() + 3); // Skip reserved bytes
            int dimensionPermutation = buffer.getInt();
            buffer.position(buffer.position() + 4); // Skip reserved bytes

            int[] dimensionSizes = new int[4];
            for (int j = 0; j < 4; j++) {
                dimensionSizes[j] = buffer.getInt();
            }

            CompoundMemberDatatype compoundMemberDatatype = new CompoundMemberDatatype(name, offset, dimensionality, dimensionPermutation, dimensionSizes, DatatypeMessage.getHdfDatatype(buffer));

            members.add(compoundMemberDatatype);
        }
    }

    private static String readNullTerminatedString(ByteBuffer buffer) {
        StringBuilder nameBuilder = new StringBuilder();
        byte b;
        while ((b = buffer.get()) != 0) {
            nameBuilder.append((char) b);
        }
        return nameBuilder.toString();
    }

    private static void alignBufferTo8ByteBoundary(ByteBuffer buffer, int dataLength) {
        int padding = (8 - (dataLength % 8)) % 8;
        buffer.position(buffer.position() + padding);
    }

    // Existing private method for byte[][] conversion
    private byte[][] toByteArrayArray(byte[] bytes) {
        byte[][] result = new byte[members.size()][];
        for (int i = 0; i < members.size(); i++) {
            CompoundMemberDatatype member = members.get(i);
            int offset = member.getOffset();
            int memberSize = member.getSize();
            result[i] = Arrays.copyOfRange(bytes, offset, offset + memberSize);
        }
        return result;
    }

    // Updated private method for HdfData[] conversion
    private HdfData[] toHdfDataArray(byte[] bytes) {
        HdfData[] result = new HdfData[members.size()];
        for (int i = 0; i < members.size(); i++) {
            CompoundMemberDatatype member = members.get(i);
            int offset = member.getOffset();
            int memberSize = member.getSize();
            byte[] memberBytes = Arrays.copyOfRange(bytes, offset, offset + memberSize);
            result[i] = member.getInstance(HdfData.class, memberBytes);
        }
        return result;
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        for (CompoundMemberDatatype member : members) {
            member.writeDefinitionToByteBuffer(buffer);
        }
    }

    /**
     * Returns the datatype class for this compound datatype.
     *
     * @return DatatypeClass.COMPOUND, indicating an HDF5 compound datatype
     */
    @Override
    public HdfDatatype.DatatypeClass getDatatypeClass() {
        return DatatypeClass.COMPOUND;
    }

    /**
     * Returns a string representation of this CompoundDatatype.
     *
     * @return a string describing the datatype's bit field, size, and members
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompoundDatatype {")
                .append(" classBitField: ").append(classBitField)
                .append(", size: ").append(size)
                .append(", ");
        members.forEach(member -> {
            builder.append("\r\n");
            builder.append(member);
        });
        return builder.toString();
    }

    /**
     * Returns the size of the datatype message data.
     *
     * @return the size of the message data in bytes, as a short
     */
    @Override
    public short getSizeMessageData() {
        short size = 8; // confused here for 0 and 8
        for (CompoundMemberDatatype member : members) {
            size += member.getSizeMessageData();
        }
        return size;
    }

    /**
     * Registers a converter for transforming CompoundDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the HdfConverter for converting between CompoundDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, HdfConverter<CompoundDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    /**
     * Converts byte data to an instance of the specified class using registered converters or POJO mapping.
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @param bytes the byte array containing the data
     * @return an instance of type T created from the byte array
     * @throws UnsupportedOperationException if no suitable converter is found and POJO conversion is not applicable
     * @throws IllegalArgumentException if POJO conversion fails
     */
    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        // Check CONVERTERS first
        @SuppressWarnings("unchecked")
        HdfConverter<CompoundDatatype, T> converter = (HdfConverter<CompoundDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<CompoundDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }

        // Fall back to toPOJO for non-primitive, unregistered types
        if (!clazz.isPrimitive()) {
            try {
                return toPOJO(clazz, bytes);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to convert to POJO: " + clazz, e);
            }
        }

        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    /**
     * Indicates whether a global heap is required for this datatype.
     *
     * @param required true if the global heap is required for any member, false otherwise
     * @return true if any member requires a global heap, false otherwise
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        for (CompoundMemberDatatype member : members) {
            required = member.requiresGlobalHeap(required);
        }
        return required;
    }

    /**
     * Converts byte data to a POJO instance of the specified class using reflection.
     *
     * @param <T>   the type of the POJO to be created
     * @param clazz the Class object representing the POJO type
     * @param bytes the byte array containing the data
     * @return an instance of type T populated with data from the byte array
     * @throws RuntimeException if reflection fails or a field is not found
     */
    public <T> T toPOJO(Class<T> clazz, byte[] bytes) {
        Map<String, Field> nameToFieldMap = Arrays.stream(clazz.getDeclaredFields()).collect(Collectors.toMap(Field::getName, f -> f));
        Map<String, CompoundMemberDatatype> nameToMemberMap = members.stream().collect(Collectors.toMap(CompoundMemberDatatype::getName, compoundMember -> compoundMember));
        try {
            T instance = clazz.getDeclaredConstructor().newInstance();
            for (CompoundMemberDatatype member : nameToMemberMap.values()) {
                Field field = nameToFieldMap.get(member.getName());
                if (field == null) {
                    throw new NoSuchFieldException(member.getName());
                }
                field.setAccessible(true);
                Object value = member.getInstance(field.getType(), Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset() + member.getSize()));
                if (field.getType().isAssignableFrom(value.getClass())) {
                    field.set(instance, value);
                }
            }
            return instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets the global heap for this datatype and its members.
     *
     * @param globalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        for (CompoundMemberDatatype member : members) {
            member.setGlobalHeap(globalHeap);
        }
    }

    /**
     * Converts the byte array to a string representation of the compound datatype's members.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the members' values
     */
    @Override
    public String toString(byte[] bytes) {
        return members.stream().map(m ->
                m.toString(Arrays.copyOfRange(bytes, m.getOffset(), m.getOffset() + m.getSize()))
        ).collect(Collectors.joining(", "));
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
     * Returns the total size of the compound datatype in bytes.
     *
     * @return the total size in bytes
     */
    @Override
    public int getSize() {
        return size;
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

    public List<CompoundMemberDatatype> getMembers() {
        return members;
    }
}