package com.github.karlnicholas.hdf5javalib.datatype;


import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.BitSet;

@Getter
public class FixedPointDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitOffset;
    private final short bitPrecision;

    public FixedPointDatatype(byte classAndVersion, BitSet classBitField, int size, short bitOffset, short bitPrecision) {

        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
    }

    public static FixedPointDatatype parseFixedPointType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {

        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();

//        short messageDataSize;
//        if ( name.length() > 0 ) {
//            int padding = (8 -  ((name.length()+1)% 8)) % 8;
//            messageDataSize = (short) (name.length()+1 + padding + 44);
//        } else {
//            messageDataSize = 44;
//        }
//        short messageDataSize = 8;

        return new FixedPointDatatype(classAndVersion, classBitField, size, bitOffset, bitPrecision);
    }

    public static BitSet createClassBitField(boolean bigEndian, boolean loPad, boolean hiPad, boolean signed) {
        BitSet classBitField = new BitSet();
        if (bigEndian) classBitField.set(0);
        if (loPad) classBitField.set(1);
        if (hiPad) classBitField.set(2);
        if (signed) classBitField.set(3);
        return classBitField;
    }

    public static byte createClassAndVersion() {
        return 0x10;
    }

    public HdfFixedPoint getInstance(ByteBuffer dataBuffer) {
        return HdfFixedPoint.readFromByteBuffer(dataBuffer, size, classBitField.get(3));
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort(bitOffset);         // 4
        buffer.putShort(bitPrecision);      // 4
    }

    @Override
    public DatatypeClass getDatatypeClass() {
            return DatatypeClass.FIXED;
    }

    @Override
    public short getSizeMessageData() {
        return (short) size;
    }

    @Override
    public String toString() {
        return "FixedPointDatatype{" +
                "classAndVersion=" + classAndVersion +
                ", classBitField=" + classBitField +
                ", size=" + size +
                ", bitOffset=" + bitOffset +
                ", bitPrecision=" + bitPrecision +
                '}';
    }

    public boolean isSigned() {
        return classBitField.get(3);
    }

    public boolean isBigEndian() {
        return classBitField.get(0);
    }
}

