package com.github.karlnicholas.hdf5javalib.datatype;

import com.github.karlnicholas.hdf5javalib.data.HdfString;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.BitSet;

@Getter
public class StringDatatype implements HdfDatatype {
    private final byte version;
    private final int size;
    private final int paddingType;
    private final String paddingDescription;
    private final int charSet;
    private final String charSetDescription;
    private final short sizeMessageData;

    public StringDatatype(byte version, int size, int paddingType, String paddingDescription, int charSet, String charSetDescription, short sizeMessageData) {
        this.version = version;
        this.size = size;
        this.paddingType = paddingType;
        this.paddingDescription = paddingDescription;
        this.charSet = charSet;
        this.charSetDescription = charSetDescription;
        this.sizeMessageData = sizeMessageData;
    }


    public static StringDatatype parseString(byte version, BitSet classBitField, int size) {
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

//        int padding = (8 -  ((name.length()+1)% 8)) % 8;
//        short messageDataSize = (short) (name.length()+1 + padding + 40);
        short messageDataSize = (short) 40;

        return new StringDatatype(version, size, paddingType, paddingDescription, charSet, charSetDescription, messageDataSize);
    }

    private static int extractBits(BitSet bitSet, int start, int end) {
        int value = 0;
        for (int i = start; i <= end; i++) {
            if (bitSet.get(i)) {
                value |= (1 << (i - start));
            }
        }
        return value;
    }

    public HdfString getInstance(ByteBuffer dataBuffer) {
        byte[] bytes = new byte[size];
        dataBuffer.get(bytes);
        return new HdfString(bytes, paddingType > 0 , charSet > 0 );
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.STRING;
    }

    @Override
    public BitSet getClassBitBytes() {
        return new BitSet();
    }

    @Override
    public String toString() {
        return "StringDatatype{" +
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
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // class and version
        buffer.put((byte)(version << 4 | 0b11));
        byte[] classBits = new byte[3];
        buffer.put(classBits);
        buffer.putInt(size);
    }

}

