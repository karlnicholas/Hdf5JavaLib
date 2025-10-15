package org.hdf5javalib.datatype;

import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.hdffile.dataobjects.messages.DatatypeMessage;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Represents an HDF5 Compound Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code CompoundDatatype} class models a compound datatype in HDF5, consisting of multiple member datatypes.
 * It supports parsing from a {@link ByteBuffer}, conversion to Java types such as {@link HdfCompound},
 * {@code HdfData[]}, or {@code String}, and mapping to POJOs using registered converters or reflection, as per
 * the HDF5 compound datatype (class 6).
 * </p>
 *
 * @see Datatype
 * @see HdfGlobalHeap
 * @see DatatypeMessage
 */
public class CompoundDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 6, version 1).
     */
    private final int classAndVersion;
    /**
     * A BitSet indicating the number of members in the compound datatype.
     */
    private final BitSet classBitField;
    /**
     * The total size of the compound datatype in bytes.
     */
    private final int size;
    /**
     * The list of member datatypes defining the compound structure.
     */
    private List<CompoundMemberDatatype> members;
    private final HdfDataFile dataFile;

    // Add these static fields to your CompoundDatatype class
    private static final Map<Class<?>, Map<String, Field>> CLASS_FIELD_CACHE = new ConcurrentHashMap<>();
    private static final Map<Class<?>, Constructor<?>> CLASS_CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();
    private static final Map<Class<?>, RecordComponent[]> CLASS_RECORD_COMPONENTS_CACHE = new ConcurrentHashMap<>();

    // Cache the member map per instance (since members don't change after construction)
    private volatile Map<String, CompoundMemberDatatype> cachedMemberMap;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<CompoundDatatype, ?>> CONVERTERS = new HashMap<>();

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
    public CompoundDatatype(int classAndVersion, BitSet classBitField, int size, List<CompoundMemberDatatype> members, HdfDataFile dataFile) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.members = new ArrayList<>(members); // Deep copy to avoid external modification
        this.dataFile = dataFile;
    }

    /**
     * Constructs a CompoundDatatype by parsing from a ByteBuffer.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   a BitSet indicating the number of members
     * @param size            the total size of the compound datatype in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     */
    public CompoundDatatype(int classAndVersion, BitSet classBitField, int size, ByteBuffer buffer, HdfDataFile dataFile) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.dataFile = dataFile;
        readFromByteBuffer(buffer, dataFile);
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

    private void readFromByteBuffer(ByteBuffer buffer, HdfDataFile hdfDataFile) {
        this.members = new ArrayList<>();
        byte version = (byte) (classAndVersion >>> 4 & 0x0F);
        int numberOfMembers = extractNumberOfMembersFromBitSet();
        for (int i = 0; i < numberOfMembers; i++) {
            buffer.mark();
            String name = HdfReadUtils.readNullTerminatedString(buffer);

            long offset;
            if ( version != 3) {
                // Align to 8-byte boundary
                alignBufferTo8ByteBoundary(buffer, name.length() + 1);
                offset = buffer.getInt();
            } else {
                //TODO:Assuming I need to check this for all datatypes
                offset = readNumberOfMembers(buffer, numberOfMembers);
            }

            int dimensionality;
            int[] dimensionSizes = new int[4];
            int dimensionPermutation = 0;
            if (version == 1) {
                dimensionality = Byte.toUnsignedInt(buffer.get());
                buffer.position(buffer.position() + 3); // Skip reserved bytes
                buffer.getInt();
                buffer.position(buffer.position() + 4); // Skip reserved bytes

                for (int j = 0; j < 4; j++) {
                    dimensionSizes[j] = buffer.getInt();
                }

            } else if (version == 2 || version == 3) {
                dimensionality = 0;
                for (int j = 0; j < 4; j++) {
                    dimensionSizes[j] = 0;
                }

            } else {
                throw new UnsupportedOperationException("Unsupported classAndVersion: " + classAndVersion);
            }


            CompoundMemberDatatype compoundMemberDatatype = new CompoundMemberDatatype(
                    name,
                    offset,
                    dimensionality,
                    dimensionPermutation,
                    dimensionSizes,
                    DatatypeMessage.getHdfDatatype(buffer, hdfDataFile),
                    hdfDataFile
            );

            members.add(compoundMemberDatatype);
        }
    }

    public static long readNumberOfMembers(ByteBuffer buffer, long elementSize) {
//        // Set ByteBuffer to Little Endian
//        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Determine the field size based on elementSize
        int fieldSize;
        if (elementSize < 256) {
            fieldSize = 1; // 1 byte
        } else if (elementSize < 65536) {
            fieldSize = 2; // 2 bytes
        } else if (elementSize < 16777216) {
            fieldSize = 3; // 3 bytes
        } else if (elementSize < 4294967296L) {
            fieldSize = 4; // 4 bytes
        } else {
            fieldSize = 8; // 8 bytes
        }

        // Read the appropriate number of bytes and convert to long
        long numberOfMembers;
        switch (fieldSize) {
            case 1:
                numberOfMembers = buffer.get() & 0xFF; // Read 1 byte, unsigned
                break;
            case 2:
                numberOfMembers = buffer.getShort() & 0xFFFF; // Read 2 bytes, unsigned
                break;
            case 3:
                // Read 3 bytes manually (ByteBuffer doesn't have a direct 3-byte read)
                byte[] bytes3 = new byte[3];
                buffer.get(bytes3);
                numberOfMembers = ((bytes3[2] & 0xFFL) << 16) |
                        ((bytes3[1] & 0xFFL) << 8) |
                        (bytes3[0] & 0xFFL);
                break;
            case 4:
                numberOfMembers = buffer.getInt() & 0xFFFFFFFFL; // Read 4 bytes, unsigned
                break;
            case 8:
                numberOfMembers = buffer.getLong(); // Read 8 bytes
                break;
            default:
                throw new IllegalStateException("Invalid field size: " + fieldSize);
        }

        return numberOfMembers;
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
            long offset = member.getOffset();
            int memberSize = member.getSize();
            result[i] = Arrays.copyOfRange(bytes, Math.toIntExact(offset), Math.toIntExact(offset + memberSize));
        }
        return result;
    }

    // Updated private method for HdfData[] conversion
    private HdfData[] toHdfDataArray(byte[] bytes) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        HdfData[] result = new HdfData[members.size()];
        for (int i = 0; i < members.size(); i++) {
            CompoundMemberDatatype member = members.get(i);
            long offset = member.getOffset();
            int memberSize = member.getSize();
            byte[] memberBytes = Arrays.copyOfRange(bytes, Math.toIntExact(offset), Math.toIntExact(offset + memberSize));
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
    public DatatypeClass getDatatypeClass() {
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
                .append(" classAndVersion: ").append(classAndVersion)
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
    public int getSizeMessageData() {
        short hdrSize = 8; // Header size
        for (CompoundMemberDatatype member : members) {
            hdrSize += member.getSizeMessageData();
        }
        return hdrSize;
    }

    /**
     * Registers a converter for transforming CompoundDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between CompoundDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<CompoundDatatype, T> converter) {
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
     * @throws IllegalArgumentException      if POJO conversion fails
     */
    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        // Check CONVERTERS first
        @SuppressWarnings("unchecked")
        DatatypeConverter<CompoundDatatype, T> converter = (DatatypeConverter<CompoundDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<CompoundDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }

        // Fall back to toPOJO for non-primitive, unregistered types
        if (!clazz.isPrimitive()) {
            return toPOJO(clazz, bytes);
        }

        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    /**
     * Converts byte data to a POJO or record instance of the specified class using reflection.
     *
     * @param <T>   the type of the POJO or record to be created
     * @param clazz the Class object representing the type
     * @param bytes the byte array containing the data
     * @return an instance of type T populated with data from the byte array
     * @throws RuntimeException if reflection fails or a field/constructor is not found
     */
    public <T> T toPOJO(Class<T> clazz, byte[] bytes) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Byte array cannot be null or empty");
        }

        // Get cached field map for the class
        Map<String, Field> nameToFieldMap = CLASS_FIELD_CACHE.computeIfAbsent(clazz, c ->
                Arrays.stream(c.getDeclaredFields())
                        .collect(Collectors.toMap(Field::getName, f -> f)));

        // Get cached member map (instance-level cache since members are per-instance)
        Map<String, CompoundMemberDatatype> nameToMemberMap = getCachedMemberMap();

        T instance;
        if (clazz.isRecord()) {
            instance = createRecordInstance(clazz, bytes, nameToMemberMap);
        } else {
            instance = createClassInstance(clazz, bytes, nameToFieldMap, nameToMemberMap);
        }
        return instance;
    }

    // Helper method to get cached member map
    private Map<String, CompoundMemberDatatype> getCachedMemberMap() {
        if (cachedMemberMap == null) {
            synchronized (this) {
                if (cachedMemberMap == null) {
                    cachedMemberMap = members.stream()
                            .collect(Collectors.toMap(CompoundMemberDatatype::getName, m -> m));
                }
            }
        }
        return cachedMemberMap;
    }

    // Extracted record creation logic with caching
    private <T> T createRecordInstance(Class<T> clazz, byte[] bytes,
                                       Map<String, CompoundMemberDatatype> nameToMemberMap) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {

        // Cache record components
        RecordComponent[] components = CLASS_RECORD_COMPONENTS_CACHE.computeIfAbsent(clazz,
                Class::getRecordComponents);

        Class<?>[] paramTypes = Arrays.stream(components)
                .map(RecordComponent::getType)
                .toArray(Class<?>[]::new);

        // Cache constructor
        @SuppressWarnings("unchecked")
        Constructor<T> constructor = (Constructor<T>) CLASS_CONSTRUCTOR_CACHE.computeIfAbsent(clazz, c -> {
            try {
                Constructor<T> ctor = clazz.getDeclaredConstructor(paramTypes);
                ctor.setAccessible(true);
                return ctor;
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("No canonical constructor found for record: " + clazz.getName(), e);
            }
        });

        Object[] args = new Object[paramTypes.length];
        for (int i = 0; i < components.length; i++) {
            String componentName = components[i].getName();
            CompoundMemberDatatype member = nameToMemberMap.get(componentName);
            if (member != null) {
                Object value = member.getInstance(paramTypes[i], Arrays.copyOfRange(
                        bytes, Math.toIntExact(member.getOffset()), Math.toIntExact(member.getOffset() + member.getSize())));
                if (paramTypes[i].isAssignableFrom(value.getClass())) {
                    args[i] = value;
                } else {
                    args[i] = getDefaultValue(paramTypes[i]);
                }
            } else {
                args[i] = getDefaultValue(paramTypes[i]);
            }
        }
        return constructor.newInstance(args);
    }

    // Extracted class creation logic with caching
    private <T> T createClassInstance(Class<T> clazz, byte[] bytes,
                                      Map<String, Field> nameToFieldMap,
                                      Map<String, CompoundMemberDatatype> nameToMemberMap) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {

        // Cache no-arg constructor
        @SuppressWarnings("unchecked")
        Constructor<T> constructor = (Constructor<T>) CLASS_CONSTRUCTOR_CACHE.computeIfAbsent(clazz, c -> {
            try {
                return clazz.getDeclaredConstructor();
            } catch (NoSuchMethodException e) {
                // Fallback to parameterized constructor
                Constructor<?>[] constructors = clazz.getDeclaredConstructors();
                Constructor<?> fallback = Arrays.stream(constructors)
                        .filter(ct -> ct.getParameterCount() <= nameToMemberMap.size())
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No suitable constructor for " + clazz.getName()));
                return fallback;
            }
        });

        T instance;
        if (constructor.getParameterCount() == 0) {
            instance = constructor.newInstance();
        } else {
            // Handle parameterized constructor
            Class<?>[] paramTypes = constructor.getParameterTypes();
            Object[] args = new Object[paramTypes.length];
            for (int i = 0; i < paramTypes.length; i++) {
                args[i] = getDefaultValue(paramTypes[i]);
            }
            instance = constructor.newInstance(args);
        }

        // Set fields - this part benefits from cached field map
        for (CompoundMemberDatatype member : nameToMemberMap.values()) {
            Field field = nameToFieldMap.get(member.getName());
            if (field != null) {
                Object value = member.getInstance(field.getType(), Arrays.copyOfRange(
                        bytes, Math.toIntExact(member.getOffset()), Math.toIntExact(member.getOffset() + member.getSize())));
                if (field.getType().isAssignableFrom(value.getClass())) {
                    field.setAccessible(true);
                    field.set(instance, value);
                }
            }
        }
        return instance;
    }

    // Optional: Method to clear caches if needed (useful for testing or memory management)
    public static void clearReflectionCaches() {
        CLASS_FIELD_CACHE.clear();
        CLASS_CONSTRUCTOR_CACHE.clear();
        CLASS_RECORD_COMPONENTS_CACHE.clear();
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
    public String toString(byte[] bytes) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        StringJoiner joiner = new StringJoiner(", ");
        for (CompoundMemberDatatype m : members) {
            String string = m.toString(Arrays.copyOfRange(bytes, Math.toIntExact(m.getOffset()), Math.toIntExact(m.getOffset() + m.getSize())));
            joiner.add(string);
        }
        return joiner.toString();
    }

    @Override
    public HdfDataFile getDataFile() {
        return dataFile;
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

    // Helper to provide default values for parameters or fields, generic for any type
    private Object getDefaultValue(Class<?> type) {
        // Primitives
        if (type == int.class) return 0;
        if (type == float.class) return 0.0f;
        if (type == double.class) return 0.0;
        if (type == long.class) return 0L;
        if (type == short.class) return (short) 0;
        if (type == byte.class) return (byte) 0;
        if (type == char.class) return (char) 0;
        if (type == boolean.class) return false;

        // Arrays
        if (type.isArray()) {
            return Array.newInstance(type.getComponentType(), 0); // Zero-length array
        }

        // Enums
        if (type.isEnum()) {
            Object[] constants = type.getEnumConstants();
            return constants.length > 0 ? constants[0] : null; // First enum constant or null
        }

        // Records
        if (type.isRecord()) {
            try {
                RecordComponent[] components = type.getRecordComponents();
                Class<?>[] paramTypes = Arrays.stream(components)
                        .map(RecordComponent::getType)
                        .toArray(Class<?>[]::new);
                Constructor<?> constructor = type.getDeclaredConstructor(paramTypes);
                Object[] args = new Object[paramTypes.length];
                for (int i = 0; i < paramTypes.length; i++) {
                    args[i] = getDefaultValue(paramTypes[i]); // Recursive call for nested types
                }
                return constructor.newInstance(args);
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException |
                     InvocationTargetException e) {
                return null; // Fallback to null if instantiation fails
            }
        }

        // Other reference types (e.g., String, List, custom classes)
        return null;
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return members.stream().flatMap(m -> m.getReferenceInstances().stream()).toList();
    }
}