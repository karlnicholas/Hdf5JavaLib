package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfEnum;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Getter
public class EnumDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;            // Size of each value (matches base type)
    private final HdfDatatype baseType;// Parent/base datatype (usually integer)
    private final String[] names;      // Array of enumeration names
    private final byte[] values;       // Packed values matching the base type size

    private static final Map<Class<?>, HdfConverter<EnumDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfEnum.class, HdfEnum::new);
        CONVERTERS.put(HdfData.class, HdfEnum::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    public EnumDatatype(byte classAndVersion, BitSet classBitField, int size,
                        HdfDatatype baseType, String[] names, byte[] values) {
        if (names.length != getNumberOfMembers(classBitField)) {
            throw new IllegalArgumentException("Number of names doesn't match classBitField specification");
        }
        if (values.length != names.length * size) {
            throw new IllegalArgumentException("Values array length doesn't match expected size");
        }
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.baseType = baseType;
        this.names = names.clone();
        this.values = values.clone();
    }

    public static EnumDatatype parseEnumDatatype(byte classAndVersion, BitSet classBitField,
                                                 int size, ByteBuffer buffer) {
        // Base type is parsed from the buffer first, after size
        HdfDatatype baseType = DatatypeMessage.getHdfDatatype(buffer);

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

        return new EnumDatatype(classAndVersion, classBitField, size, baseType, names, values);
    }

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

    public static byte createClassAndVersion(int version) {
        if (version != 1 && version != 2) {
            throw new IllegalArgumentException("Enum Datatype only supports versions 1 and 2");
        }
        return (byte) ((8 << 4) | version); // Class 8, specified version
    }

    public static int getNumberOfMembers(BitSet classBitField) {
        int num = 0;
        for (int i = 0; i < 16; i++) {
            if (classBitField.get(i)) {
                num |= 1 << i;
            }
        }
        return num;
    }

    public static <T> void addConverter(Class<T> clazz, HdfConverter<EnumDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<EnumDatatype, T> converter = (HdfConverter<EnumDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<EnumDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    public String toString(byte[] bytes) {
        int valueIndex = findValueIndex(bytes);
        return valueIndex >= 0 ? names[valueIndex] : "undefined";
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

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.ENUM;
    }

    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    @Override
    public short getSizeMessageData() {
        int totalNameBytes = 0;
        for (String name : names) {
            int nameLength = name.getBytes(StandardCharsets.US_ASCII).length + 1; // Include null terminator
            totalNameBytes += (nameLength + 7) & ~7; // Round up to next 8-byte multiple
        }
        return (short) (totalNameBytes + values.length + baseType.getSize());
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // Write base type definition first
        baseType.writeDefinitionToByteBuffer(buffer);
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

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        // Empty implementation to satisfy interface
    }

    @Override
    public String toString() {
        return "EnumDatatype{" +
                "size=" + size +
                ", numMembers=" + names.length +
                ", baseType=" + baseType +
                '}';
    }
}