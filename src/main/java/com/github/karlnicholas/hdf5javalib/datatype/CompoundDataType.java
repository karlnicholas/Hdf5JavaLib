package com.github.karlnicholas.hdf5javalib.datatype;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

@Getter
public class CompoundDataType implements HdfDataType {
    private final int numberOfMembers; // Number of members in the compound datatype
    private final int size;
    private List<Member> members;     // Member definitions

    // New application-level constructor
    public CompoundDataType(int numberOfMembers, int size, List<Member> members) {
        this.numberOfMembers = numberOfMembers;
        this.size = size;
        this.members = new ArrayList<>(members); // Deep copy to avoid external modification
    }

    public CompoundDataType(BitSet classBitField, int size, byte[] data) {
        this.numberOfMembers = extractNumberOfMembersFromBitSet(classBitField);
        this.size = size;
        ByteBuffer cdtcBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        readFromByteBuffer(cdtcBuffer);
    }

    private short extractNumberOfMembersFromBitSet(BitSet classBitField) {
        short value = 0;
        for (int i = 0; i < classBitField.length(); i++) {
            if (classBitField.get(i)) {
                value |= (1 << i);
            }
        }
        return value;
    }

    private void readFromByteBuffer(ByteBuffer buffer) {
        this.members = new ArrayList<>();
//        buffer.position(8);
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

            CompoundTypeMember type = parseMemberDataType(buffer, name);

            members.add(new Member(name, offset, dimensionality, dimensionPermutation, dimensionSizes, type));
        }
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        for (Member member: members) {
            buffer.put(member.getName().getBytes(StandardCharsets.US_ASCII));
            buffer.put((byte)0);
            int paddingSize = (8 -  ((member.getName().length()+1)% 8)) % 8;
            buffer.put(new byte[paddingSize]);
            buffer.putInt(member.getOffset());
            buffer.put((byte)member.dimensionality);
            buffer.put(new byte[3]);
            buffer.putInt(member.dimensionPermutation);
            buffer.put(new byte[4]);
            for( int ds: member.dimensionSizes) {
                buffer.putInt(ds);
            }
            member.type.writeToByteBuffer(buffer);

        }
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        // Intentionally left blank
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

    private CompoundTypeMember parseMemberDataType(ByteBuffer buffer, String name) {
        byte classAndVersion = buffer.get();
        byte version = (byte) ((classAndVersion >> 4) & 0x0F);
        int dataTypeClass = classAndVersion & 0x0F;

        byte[] classBits = new byte[3];
        buffer.get(classBits);
        BitSet classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        short size = (short) Integer.toUnsignedLong(buffer.getInt());

        return switch (dataTypeClass) {
            case 0 -> parseFixedPoint(buffer, version, size, classBitField, name);
            case 1 -> parseFloatingPoint(buffer, version, size, classBitField, name);
            case 3 -> parseString(version, size, classBitField, name);
            default -> throw new UnsupportedOperationException("Unsupported datatype class: " + dataTypeClass);
        };
    }

    private FixedPointMember parseFixedPoint(ByteBuffer buffer, byte version, short size, BitSet classBitField, String name) {
        boolean bigEndian = classBitField.get(0);
        boolean loPad = classBitField.get(1);
        boolean hiPad = classBitField.get(2);
        boolean signed = classBitField.get(3);

        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();

        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        short messageDataSize = (short) (name.length()+1 + padding + 44);

        return new FixedPointMember(version, size, bigEndian, loPad, hiPad, signed, bitOffset, bitPrecision, messageDataSize, classBitField);
    }

    private FloatingPointMember parseFloatingPoint(ByteBuffer buffer, byte version, short size, BitSet classBitField, String name) {
        boolean bigEndian = classBitField.get(0);
        int exponentBits = buffer.getInt();
        int mantissaBits = buffer.getInt();
        return new FloatingPointMember(version, size, exponentBits, mantissaBits, bigEndian);
    }

    private StringMember parseString(byte version, short size, BitSet classBitField, String name) {
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

        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        short messageDataSize = (short) (name.length()+1 + padding + 40);

        return new StringMember(version, size, paddingType, paddingDescription, charSet, charSetDescription, messageDataSize);
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

    @Override
    public short getSizeMessageData() {
        short size = 0;
        for(Member member: members) {
            size += member.getType().getSizeMessageData();
        }
        return size;
    }

    public interface CompoundTypeMember {
        short getSizeMessageData();

        short getSize();

        void writeToByteBuffer(ByteBuffer buffer);
    }
    public static class Member {
        @Getter
        private final String name;
        @Getter
        private final int offset;
        private final int dimensionality;
        private final int dimensionPermutation;
        private final int[] dimensionSizes;
        @Getter
        private final CompoundTypeMember type;

        public Member(String name, int offset, int dimensionality, int dimensionPermutation, int[] dimensionSizes, CompoundTypeMember type) {
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

    @Getter
    public static class FixedPointMember implements  CompoundTypeMember {
        private final byte version;
        private final BitSet classBitField;
        private final short size;
        private final boolean bigEndian;
        private final boolean loPad;
        private final boolean hiPad;
        private final boolean signed;
        private final short bitOffset;
        private final short bitPrecision;
        private final short sizeMessageData;

        public FixedPointMember(byte version, short size, boolean bigEndian, boolean loPad, boolean hiPad, boolean signed, short bitOffset, short bitPrecision, short sizeMessageData, BitSet classBitField) {
            this.version = version;
            this.size = size;
            this.bigEndian = bigEndian;
            this.loPad = loPad;
            this.hiPad = hiPad;
            this.signed = signed;
            this.bitOffset = bitOffset;
            this.bitPrecision = bitPrecision;
            this.sizeMessageData = sizeMessageData;
            this.classBitField = classBitField;
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

        public HdfFixedPoint getInstance(ByteBuffer dataBuffer) {
            return HdfFixedPoint.readFromByteBuffer(dataBuffer, size, signed);
        }

        @Override
        public short getSizeMessageData() {
            return sizeMessageData;
        }

        @Override
        public void writeToByteBuffer(ByteBuffer buffer) {
            // class and version
            // class and version
            buffer.put((byte)(version << 4 | 0b0));
            byte[] classBits = new byte[3];
            buffer.put(classBits);
            buffer.putInt(size);

            buffer.putShort(bitOffset);
            buffer.putShort(bitPrecision);
        }
    }

    @Getter
    public static class StringMember implements CompoundTypeMember {
        private final byte version;
        private final short size;
        private final int paddingType;
        private final String paddingDescription;
        private final int charSet;
        private final String charSetDescription;
        private final short sizeMessageData;

        public StringMember(byte version, short size, int paddingType, String paddingDescription, int charSet, String charSetDescription, short sizeMessageData) {
            this.version = version;
            this.size = size;
            this.paddingType = paddingType;
            this.paddingDescription = paddingDescription;
            this.charSet = charSet;
            this.charSetDescription = charSetDescription;
            this.sizeMessageData = sizeMessageData;
        }

        public HdfString getInstance(ByteBuffer dataBuffer) {
            byte[] bytes = new byte[(int)size];
            dataBuffer.get(bytes);
            return new HdfString(bytes, paddingType > 0 , charSet > 0 );
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

        @Override
        public short getSizeMessageData() {
            return sizeMessageData;
        }
        @Override
        public void writeToByteBuffer(ByteBuffer buffer) {
            // class and version
            buffer.put((byte)(version << 4 | 0b11));
            byte[] classBits = new byte[3];
            buffer.put(classBits);
            buffer.putInt(size);
        }
    }

    @Getter
    public static class FloatingPointMember implements CompoundTypeMember {
        private final byte version;
        private final short size;
        private final int exponentBits;
        private final int mantissaBits;
        private final boolean bigEndian;

        public FloatingPointMember(byte version, short size, int exponentBits, int mantissaBits, boolean bigEndian) {
            this.version = version;
            this.size = size;
            this.exponentBits = exponentBits;
            this.mantissaBits = mantissaBits;
            this.bigEndian = bigEndian;
        }

        public Object getInstance() {
            return new Object();
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

        @Override
        public short getSizeMessageData() {
            return size;
        }
        @Override
        public void writeToByteBuffer(ByteBuffer buffer) {
            buffer.putInt(exponentBits);
            buffer.putInt(mantissaBits);
        }
    }
}
