package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class VariableLengthDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final int baseType;
    private HdfGlobalHeap globalHeap;
    // In your HdfDataType/FixedPointDatatype class
    private static final Map<Class<?>, HdfConverter<VariableLengthDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, ((bytes, datatype) -> datatype.toString(bytes)));
        CONVERTERS.put(HdfVariableLength.class, HdfVariableLength::new);
        CONVERTERS.put(HdfData.class, HdfVariableLength::new);
    }

    private Object toString(byte[] bytes) {
        return new String(bytes, CharacterSet.fromBitSet(classBitField) == CharacterSet.ASCII ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8);
    }

    public VariableLengthDatatype(byte classAndVersion, BitSet classBitField, int size, int baseType) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.baseType = baseType;
    }


    public static VariableLengthDatatype parseVariableLengthDatatype(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        buffer.position(buffer.position()+12);
        int baseType = 0;

        return new VariableLengthDatatype(classAndVersion, classBitField, size, baseType);
    }

    public static BitSet createClassBitField(PaddingType paddingType, CharacterSet charSet) {
        return BitSet.valueOf(new long[] {((long) charSet.value << 4) + paddingType.value});
    }

    public static byte createClassAndVersion() {
        return 0x13;
    }

    // Public method to add user-defined converters
    public static <T> void addConverter(Class<T> clazz, HdfConverter<VariableLengthDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<VariableLengthDatatype, T> converter = (HdfConverter<VariableLengthDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<VariableLengthDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, ByteBuffer buffer) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return getInstance(clazz, bytes);
    }

//    public String toString(byte[] bytes) {
//        byte[] workingBytes = new byte[size];
//        int workingEnd = Math.min(size, bytes.length);
//        System.arraycopy(bytes, 0, workingBytes, 0, workingEnd);
//
//        // Pad with spaces if SPACE_PAD and bytes is shorter than size
//        if (getPaddingType() == PaddingType.SPACE_PAD && workingEnd < size) {
//            Arrays.fill(workingBytes, workingEnd, size, (byte) ' ');
//        }
//
//        // Count non-0x00 bytes
//        int validLength = 0;
//        for (int i = 0; i < size; i++) {
//            if (workingBytes[i] != 0x00) {
//                validLength++;
//            }
//        }
//
//        // Build array without 0x00
//        byte[] cleanBytes = new byte[validLength];
//        int pos = 0;
//        for (int i = 0; i < size; i++) {
//            if (workingBytes[i] != 0x00) {
//                cleanBytes[pos++] = workingBytes[i];
//            }
//        }
//
//        return new String(cleanBytes,
//                getCharacterSet() == CharacterSet.ASCII ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8);
//
//    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.VLEN;
    }

    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    @Override
    public short getSizeMessageData() {
        return 0;
    }


    @Override
    public String toString() {
        return "VariableLengthDatatype{" +
                "size=" + size +
                ", type='" + (classBitField.get(0) ? "String" : "Sequence") + '\'' +
                ", padding='" + getPaddingType().name + '\'' +
                ", charSet='" + getCharacterSet().name + '\'' +
                '}';
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
    }

    // Inner Enum for Padding Type (Bits 0-3)
    @Getter
    public enum PaddingType {
        NULL_TERMINATE(0, "Null Terminate",
                "A zero byte marks the end of the string and is guaranteed to be present after converting a long string to a short string. " +
                        "When converting a short string to a long string, the value is padded with additional null characters as necessary."),
        NULL_PAD(1, "Null Pad",
                "Null characters are added to the end of the value during conversions from short values to long values, " +
                        "but conversion in the opposite direction simply truncates the value."),
        SPACE_PAD(2, "Space Pad",
                "Space characters are added to the end of the value during conversions from short values to long values, " +
                        "but conversion in the opposite direction simply truncates the value. This is the Fortran representation of the string."),
        RESERVED_3(3, "Reserved", "Reserved for future use."),
        RESERVED_4(4, "Reserved", "Reserved for future use."),
        RESERVED_5(5, "Reserved", "Reserved for future use."),
        RESERVED_6(6, "Reserved", "Reserved for future use."),
        RESERVED_7(7, "Reserved", "Reserved for future use."),
        RESERVED_8(8, "Reserved", "Reserved for future use."),
        RESERVED_9(9, "Reserved", "Reserved for future use."),
        RESERVED_10(10, "Reserved", "Reserved for future use."),
        RESERVED_11(11, "Reserved", "Reserved for future use."),
        RESERVED_12(12, "Reserved", "Reserved for future use."),
        RESERVED_13(13, "Reserved", "Reserved for future use."),
        RESERVED_14(14, "Reserved", "Reserved for future use."),
        RESERVED_15(15, "Reserved", "Reserved for future use.");

        private final int value;
        private final String name;
        private final String description;

        PaddingType(int value, String name, String description) {
            this.value = value;
            this.name = name;
            this.description = description;
        }

        public static PaddingType fromValue(int value) {
            for (PaddingType type : values()) {
                if (type.value == value) return type;
            }
            throw new IllegalArgumentException("Invalid padding type value: " + value);
        }

        public static PaddingType fromBitSet(BitSet bits) {
            int value = (bits.get(4) ? 1 : 0) | (bits.get(5) ? 2 : 0) |
                    (bits.get(6) ? 4 : 0) | (bits.get(7) ? 8 : 0);
            return fromValue(value);
        }
    }

    // Inner Enum for Character Set (Bits 4-7, assumed per HDF5 spec)
    @Getter
    public enum CharacterSet {
        ASCII(0, "ASCII", "American Standard Code for Information Interchange"),
        UTF8(1, "UTF-8", "Unicode Transformation Format, 8-bit"),
        RESERVED_2(2, "Reserved", "Reserved for future use."),
        RESERVED_3(3, "Reserved", "Reserved for future use."),
        RESERVED_4(4, "Reserved", "Reserved for future use."),
        RESERVED_5(5, "Reserved", "Reserved for future use."),
        RESERVED_6(6, "Reserved", "Reserved for future use."),
        RESERVED_7(7, "Reserved", "Reserved for future use."),
        RESERVED_8(8, "Reserved", "Reserved for future use."),
        RESERVED_9(9, "Reserved", "Reserved for future use."),
        RESERVED_10(10, "Reserved", "Reserved for future use."),
        RESERVED_11(11, "Reserved", "Reserved for future use."),
        RESERVED_12(12, "Reserved", "Reserved for future use."),
        RESERVED_13(13, "Reserved", "Reserved for future use."),
        RESERVED_14(14, "Reserved", "Reserved for future use."),
        RESERVED_15(15, "Reserved", "Reserved for future use.");

        private final int value;
        private final String name;
        private final String description;

        CharacterSet(int value, String name, String description) {
            this.value = value;
            this.name = name;
            this.description = description;
        }

        public static CharacterSet fromValue(int value) {
            for (CharacterSet set : values()) {
                if (set.value == value) return set;
            }
            throw new IllegalArgumentException("Invalid character set value: " + value);
        }

        public static CharacterSet fromBitSet(BitSet bits) {
            int value = (bits.get(8) ? 1 : 0) | (bits.get(9) ? 2 : 0) |
                    (bits.get(10) ? 4 : 0) | (bits.get(11) ? 8 : 0);
            return fromValue(value);
        }
    }

    // Example usage in StringDatatype
    public PaddingType getPaddingType() {
        return PaddingType.fromBitSet(classBitField);
    }

    public CharacterSet getCharacterSet() {
        return CharacterSet.fromBitSet(classBitField);
    }

    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        this.globalHeap = globalHeap;
    }

}

