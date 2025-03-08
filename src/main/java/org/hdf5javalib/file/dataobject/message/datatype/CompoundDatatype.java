package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.*;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype.parseFixedPointType;
import static org.hdf5javalib.file.dataobject.message.datatype.FloatingPointDatatype.parseFloatingPointType;
import static org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype.DatatypeClass.FIXED;
import static org.hdf5javalib.file.dataobject.message.datatype.StringDatatype.parseStringType;

@Getter
public class CompoundDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField; // Number of members in the compound datatype
    private final int size;
    private List<CompoundMemberDatatype> members;     // Member definitions

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

//    public CompoundDatatype(BitSet classBitField, int size, byte[] data) {
//        this.numberOfMembers = extractNumberOfMembersFromBitSet(classBitField);
//        this.size = size;
//        ByteBuffer cdtcBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
//        readFromByteBuffer(cdtcBuffer);
//    }

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
//        buffer.position(8);
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

//            HdfDatatype type = parseMessageDataType(buffer, name);

//            String name = "get parsed";
            byte classAndVersion = buffer.get();
            byte version = (byte) ((classAndVersion >> 4) & 0x0F);
            byte dataTypeClass = (byte) (classAndVersion & 0x0F);

            byte[] classBits = new byte[3];
            buffer.get(classBits);
            BitSet classBitField = BitSet.valueOf(new long[]{
                    ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
            });

            int size = buffer.getInt();
            HdfDatatype hdfDatatype = parseCompoundDataType(version, dataTypeClass, classBitField, size, buffer);
            CompoundMemberDatatype compoundMemberDatatype = new CompoundMemberDatatype(name, offset, dimensionality, dimensionPermutation, dimensionSizes, hdfDatatype);

            members.add(compoundMemberDatatype);
        }
    }

    public static HdfDatatype parseCompoundDataType(byte version, byte dataTypeClass, BitSet classBitField, int size, ByteBuffer buffer) {
         return switch (dataTypeClass) {
            case 0 -> parseFixedPointType(version, classBitField, size, buffer);
            case 1 -> parseFloatingPointType(version, classBitField, size, buffer );
            case 3 -> parseStringType(version, classBitField, size, buffer);
    //            case 6 -> parseCompoundDataType(version, size, classBitField, name, buffer);
            default -> throw new UnsupportedOperationException("Unsupported datatype class: " + dataTypeClass);
        };
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


    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        for (CompoundMemberDatatype member: members) {
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
        members.forEach(member->{
            builder.append("\r\n\t");
            builder.append(member);
        });
        return builder.toString();
    }

    @Override
    public short getSizeMessageData() {
        short size = 0;
        for(CompoundMemberDatatype member: members) {
            size += member.getSizeMessageData();
        }
        return size;
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        Map<String, Field> nameToFieldMap = Arrays.stream(clazz.getDeclaredFields()).collect(Collectors.toMap(Field::getName, f -> f));
        Map<String, CompoundMemberDatatype> nameToMemberMap = members.stream().collect(Collectors.toMap(CompoundMemberDatatype::getName, compoundMember -> compoundMember));
        try {
            T instance = clazz.getDeclaredConstructor().newInstance();
            for( Field field: nameToFieldMap.values()) {
                CompoundMemberDatatype member = nameToMemberMap.get(field.getName());
                field.setAccessible(true);
                if (member != null) {
                    Object value = member.getInstance(field.getType(), bytes);
                    if (field.getType().isAssignableFrom(value.getClass())) {
                        field.set(instance, value);
                    }  // Silently skip if types don't match
                }  // Silently skip if no matching member
            }
            return instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public <T> T getInstance(Class<T> clazz, ByteBuffer buffer) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return getInstance(clazz, bytes);
    }

}
