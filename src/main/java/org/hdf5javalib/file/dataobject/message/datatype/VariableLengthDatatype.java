package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
@Slf4j
public class VariableLengthDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    @Setter
    private HdfGlobalHeap globalHeap;
    private final HdfDatatype hdfDatatype;
    // In your HdfDataType/FixedPointDatatype class
    private static final Map<Class<?>, HdfConverter<VariableLengthDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfVariableLength.class, HdfVariableLength::new);
        CONVERTERS.put(HdfData.class, HdfVariableLength::new);
        CONVERTERS.put(Object.class, (bytes, dt) -> dt.toObjectArray(bytes));
        CONVERTERS.put(byte[][].class, (bytes, dt) -> dt.toByteArrayArray(bytes)); // Raw reference bytes
    }

    public VariableLengthDatatype(byte classAndVersion, BitSet classBitField, int size, HdfDatatype hdfDatatype) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.hdfDatatype = hdfDatatype;
    }

    public static VariableLengthDatatype parseVariableLengthDatatype(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        return new VariableLengthDatatype(classAndVersion, classBitField, size, DatatypeMessage.getHdfDatatype(buffer));
    }

    public static BitSet createClassBitField(PaddingType paddingType, CharacterSet charSet) {
        return BitSet.valueOf(new long[] {((long) charSet.value << 4) + paddingType.value});
    }

    @SuppressWarnings("SameReturnValue")
    public static byte createClassAndVersion() {
        return 0x19;
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
    public boolean requiresGlobalHeap(boolean required) {
        return required | true;
    }

    public String toString(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buffer.getInt();
        long offset = buffer.getLong();
        int index = buffer.getInt();

        byte[] workingBytes = globalHeap.getDataBytes(offset, index);
        if ( getClassBitField().get(0)) {
            return new String(workingBytes, getCharacterSet() == CharacterSet.ASCII ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8);
        } else {
            int datatypeSize = hdfDatatype.getSize();
            String[] resultArray = new String[count];
            for (int i = 0; i < count; i++) {
                resultArray[i] = hdfDatatype.getInstance(String.class, Arrays.copyOfRange(workingBytes, i * datatypeSize, (i + 1 ) * datatypeSize));
            }
            return Arrays.toString(resultArray);
        }
    }

    private byte[][] toByteArrayArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buffer.getInt();
        long offset = buffer.getLong();
        int index = buffer.getInt();

        byte[] workingBytes = globalHeap.getDataBytes(offset, index);
        int datatypeSize = hdfDatatype.getSize();
        byte[][] resultArray = new byte[count][];
        for (int i = 0; i < count; i++) {
            resultArray[i] = Arrays.copyOfRange(workingBytes, i * datatypeSize, (i + 1) * datatypeSize);
        }
        return resultArray;
    }

    private Object toObjectArray(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int count = buffer.getInt();
        long offset = buffer.getLong();
        int index = buffer.getInt();

        byte[] workingBytes = globalHeap.getDataBytes(offset, index);
        int datatypeSize = hdfDatatype.getSize();
        HdfData[] result = new HdfData[count];
        for (int i = 0; i < count; i++) {
            result[i] = hdfDatatype.getInstance(HdfData.class, Arrays.copyOfRange(workingBytes, i * datatypeSize, (i + 1) * datatypeSize));
        }
        return result;
    }

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
        return 8+8;
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
        byte[] bytes = {0x13, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00};
        buffer.put(bytes);
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

}

