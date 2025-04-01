package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class CompoundDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField; // Number of members in the compound datatype
    private final int size;
    private List<CompoundMemberDatatype> members;     // Member definitions

    private static final Map<Class<?>, HdfConverter<CompoundDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfCompound.class, HdfCompound::new);
        CONVERTERS.put(HdfData.class, HdfCompound::new);
        CONVERTERS.put(byte[][].class, (bytes, dt) -> dt.toByteArrayArray(bytes));
        CONVERTERS.put(HdfData[].class, (bytes, dt) -> dt.toHdfDataArray(bytes));
    }

    // New application-level constructor
    public CompoundDatatype(byte classAndVersion, BitSet classBitField, int size, List<CompoundMemberDatatype> members) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.members = new ArrayList<>(members); // Deep copy to avoid external modification
    }

    public CompoundDatatype(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        readFromByteBuffer(buffer);
    }

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

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        for (CompoundMemberDatatype member : members) {
            member.writeDefinitionToByteBuffer(buffer);
        }
    }

    @Override
    public HdfDatatype.DatatypeClass getDatatypeClass() {
        return DatatypeClass.COMPOUND;
    }

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

    @Override
    public short getSizeMessageData() {
        short size = 0;
        for (CompoundMemberDatatype member : members) {
            size += member.getSizeMessageData();
        }
        return size;
    }

    // Public method to add user-defined converters
    public static <T> void addConverter(Class<T> clazz, HdfConverter<CompoundDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

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

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        for (CompoundMemberDatatype member : members) {
            member.setGlobalHeap(globalHeap);
        }
    }

    @Override
    public String toString(byte[] bytes) {
        return members.stream().map(m ->
                m.toString(Arrays.copyOfRange(bytes, m.getOffset(), m.getOffset() + m.getSize()))
        ).collect(Collectors.joining(", "));
    }
}