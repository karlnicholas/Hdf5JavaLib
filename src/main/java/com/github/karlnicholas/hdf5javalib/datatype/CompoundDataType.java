package com.github.karlnicholas.hdf5javalib.datatype;

import com.github.karlnicholas.hdf5javalib.message.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class CompoundDataType {
    private final int numberOfMembers; // Number of members in the compound datatype
    private final int size;
    private List<Member> members;     // Member definitions

    public CompoundDataType(DataTypeMessage dataTypeMessage, byte[] data) {
        this.numberOfMembers = extractNumberOfMembersFromBitSet(dataTypeMessage.getClassBitField());
        this.size = dataTypeMessage.getSize().getBigIntegerValue().intValue();
        ByteBuffer cdtcBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        readFromByteBuffer(cdtcBuffer);
    }

    private int extractNumberOfMembersFromBitSet(BitSet classBitField) {
        int value = 0;
        for (int i = 0; i < classBitField.length(); i++) {
            if (classBitField.get(i)) {
                value |= (1 << i);
            }
        }
        return value;
    }

    private void readFromByteBuffer(ByteBuffer buffer) {
        this.members = new ArrayList<>();
        buffer.position(8);
        for (int i = 0; i < numberOfMembers; i++) {
            String name = readNullTerminatedString(buffer);

            // Align to 8-byte boundary
            alignBufferTo8ByteBoundary(buffer, name.length() + 1);

            long offset = Integer.toUnsignedLong(buffer.getInt());
            int dimensionality = Byte.toUnsignedInt(buffer.get());
            buffer.position(buffer.position() + 3); // Skip reserved bytes
            int dimensionPermutation = buffer.getInt();
            buffer.position(buffer.position() + 4); // Skip reserved bytes

            int[] dimensionSizes = new int[4];
            for (int j = 0; j < 4; j++) {
                dimensionSizes[j] = buffer.getInt();
            }

            Object type = parseMemberDataType(buffer);

            members.add(new Member(name, offset, dimensionality, dimensionPermutation, dimensionSizes, type));
        }
    }

    private String readNullTerminatedString(ByteBuffer buffer) {
        StringBuilder nameBuilder = new StringBuilder();
        byte b;
        while ((b = buffer.get()) != 0) {
            nameBuilder.append((char) b);
        }
        return nameBuilder.toString();
    }

    private void alignBufferTo8ByteBoundary(ByteBuffer buffer, int dataLength) {
        int padding = (8 - (dataLength % 8)) % 8;
        buffer.position(buffer.position() + padding);
    }

    private Object parseMemberDataType(ByteBuffer buffer) {
        byte classAndVersion = buffer.get();
        int version = (classAndVersion >> 4) & 0x0F;
        int dataTypeClass = classAndVersion & 0x0F;

        byte[] classBits = new byte[3];
        buffer.get(classBits);
        BitSet classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        long size = Integer.toUnsignedLong(buffer.getInt());

        return switch (dataTypeClass) {
            case 0 -> parseFixedPoint(buffer, size, version, classBitField);
            case 1 -> parseFloatingPoint(buffer, size, version, classBitField);
            case 3 -> parseString(buffer, size, version, classBitField);
            default -> throw new UnsupportedOperationException("Unsupported datatype class: " + dataTypeClass);
        };
    }

    private FixedPointMember parseFixedPoint(ByteBuffer buffer, long size, int version, BitSet classBitField) {
        boolean bigEndian = classBitField.get(0);
        boolean loPad = classBitField.get(1);
        boolean hiPad = classBitField.get(2);
        boolean signed = classBitField.get(3);

        int bitOffset = Short.toUnsignedInt(buffer.getShort());
        int bitPrecision = Short.toUnsignedInt(buffer.getShort());

        return new FixedPointMember(size, bigEndian, loPad, hiPad, signed, bitOffset, bitPrecision);
    }

    private FloatingPointMember parseFloatingPoint(ByteBuffer buffer, long size, int version, BitSet classBitField) {
        boolean bigEndian = classBitField.get(0);
        int exponentBits = buffer.getInt();
        int mantissaBits = buffer.getInt();
        return new FloatingPointMember(size, exponentBits, mantissaBits, bigEndian);
    }

    private StringMember parseString(ByteBuffer buffer, long size, int version, BitSet classBitField) {
        int paddingType = extractBits(classBitField, 0, 3);
        int charSet = extractBits(classBitField, 4, 7);

        String paddingDescription = switch (paddingType) {
            case 0 -> "Null Terminate";
            case 1 -> "Null Pad";
            case 2 -> "Space Pad";
            default -> "Reserved";
        };

        String charSetDescription = switch (charSet) {
            case 0 -> "ASCII";
            case 1 -> "UTF-8";
            default -> "Reserved";
        };

        return new StringMember(size, paddingType, paddingDescription, charSet, charSetDescription);
    }

    private int extractBits(BitSet bitSet, int start, int end) {
        int value = 0;
        for (int i = start; i <= end; i++) {
            if (bitSet.get(i)) {
                value |= (1 << (i - start));
            }
        }
        return value;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompoundDataType {")
                .append(" numberOfMembers: ").append(numberOfMembers)
                .append(", size: ").append(size)
                .append(", ");
        members.forEach(member->{
            builder.append("\r\n\t");
            builder.append(member);
        });
        return builder.toString();
    }

    private static class Member {
        private final String name;
        private final long offset;
        private final int dimensionality;
        private final int dimensionPermutation;
        private final int[] dimensionSizes;
        private final Object type;

        public Member(String name, long offset, int dimensionality, int dimensionPermutation, int[] dimensionSizes, Object type) {
            this.name = name;
            this.offset = offset;
            this.dimensionality = dimensionality;
            this.dimensionPermutation = dimensionPermutation;
            this.dimensionSizes = dimensionSizes;
            this.type = type;
        }

        @Override
        public String toString() {
            return "Member{" +
                    "name='" + name + '\'' +
                    ", offset=" + offset +
                    ", dimensionality=" + dimensionality +
                    ", dimensionPermutation=" + dimensionPermutation +
                    ", dimensionSizes=" + java.util.Arrays.toString(dimensionSizes) +
                    ", type=" + type +
                    '}';
        }
    }

    public static class FixedPointMember {
        private final long size;
        private final boolean bigEndian;
        private final boolean loPad;
        private final boolean hiPad;
        private final boolean signed;
        private final int bitOffset;
        private final int bitPrecision;

        public FixedPointMember(long size, boolean bigEndian, boolean loPad, boolean hiPad, boolean signed, int bitOffset, int bitPrecision) {
            this.size = size;
            this.bigEndian = bigEndian;
            this.loPad = loPad;
            this.hiPad = hiPad;
            this.signed = signed;
            this.bitOffset = bitOffset;
            this.bitPrecision = bitPrecision;
        }

        @Override
        public String toString() {
            return "FixedPointMember{" +
                    "size=" + size +
                    ", bigEndian=" + bigEndian +
                    ", loPad=" + loPad +
                    ", hiPad=" + hiPad +
                    ", signed=" + signed +
                    ", bitOffset=" + bitOffset +
                    ", bitPrecision=" + bitPrecision +
                    '}';
        }
    }

    public static class StringMember {
        private final long size;
        private final int paddingType;
        private final String paddingDescription;
        private final int charSet;
        private final String charSetDescription;

        public StringMember(long size, int paddingType, String paddingDescription, int charSet, String charSetDescription) {
            this.size = size;
            this.paddingType = paddingType;
            this.paddingDescription = paddingDescription;
            this.charSet = charSet;
            this.charSetDescription = charSetDescription;
        }

        @Override
        public String toString() {
            return "StringMember{" +
                    "size=" + size +
                    ", paddingType=" + paddingType +
                    ", paddingDescription='" + paddingDescription + '\'' +
                    ", charSet=" + charSet +
                    ", charSetDescription='" + charSetDescription + '\'' +
                    '}';
        }
    }

    public static class FloatingPointMember {
        private final long size;
        private final int exponentBits;
        private final int mantissaBits;
        private final boolean bigEndian;

        public FloatingPointMember(long size, int exponentBits, int mantissaBits, boolean bigEndian) {
            this.size = size;
            this.exponentBits = exponentBits;
            this.mantissaBits = mantissaBits;
            this.bigEndian = bigEndian;
        }

        @Override
        public String toString() {
            return "FloatingPointMember{" +
                    "size=" + size +
                    ", exponentBits=" + exponentBits +
                    ", mantissaBits=" + mantissaBits +
                    ", bigEndian=" + bigEndian +
                    '}';
        }
    }
}
