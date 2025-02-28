package com.github.karlnicholas.hdf5javalib.message.datatype;

import com.github.karlnicholas.hdf5javalib.data.HdfString;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.BitSet;

@Getter
public class StringDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;

    public StringDatatype(byte classAndVersion, BitSet classBitField, int size) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
    }


    public static StringDatatype parseStringType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        return new StringDatatype(classAndVersion, classBitField, size);
    }

    public static BitSet getStringTypeBitSet(PaddingType paddingType, CharacterSet charSet) {
        return BitSet.valueOf(new long[] {((long) charSet.value << 4) + paddingType.value});
    }

    public static byte createClassAndVersion() {
        return 0x13;
    }

    public HdfString getInstance(ByteBuffer dataBuffer) {
        byte[] bytes = new byte[size];
        dataBuffer.get(bytes);
        return new HdfString(bytes, classBitField);
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.STRING;
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
        return "StringDatatype{" +
                "size=" + size +
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
            int value = (bits.get(0) ? 1 : 0) | (bits.get(1) ? 2 : 0) |
                    (bits.get(2) ? 4 : 0) | (bits.get(3) ? 8 : 0);
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
            int value = (bits.get(4) ? 1 : 0) | (bits.get(5) ? 2 : 0) |
                    (bits.get(6) ? 4 : 0) | (bits.get(7) ? 8 : 0);
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

