package com.github.karlnicholas.hdf5javalib.datatype;


import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.BitSet;

@Getter
public class FixedPointDatatype implements HdfDatatype {
    private final byte version;
    private final BitSet classBitField;
    private final int size;
    private final boolean bigEndian;
    private final boolean loPad;
    private final boolean hiPad;
    private final boolean signed;
    private final short bitOffset;
    private final short bitPrecision;
    private final short sizeMessageData;

    public FixedPointDatatype(byte version, int size, boolean bigEndian, boolean loPad, boolean hiPad, boolean signed, short bitOffset, short bitPrecision, short sizeMessageData, BitSet classBitField) {

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
        return "FixedPointDatatype{" +
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
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // class and version
        buffer.put((byte)(version << 4));   // 1
        byte[] classBits = new byte[3];
        byte[] currentClassBits = classBitField.toByteArray();
        System.arraycopy(currentClassBits, 0, classBits, 0, currentClassBits.length);
        buffer.put(classBits);
        buffer.putInt(size);                // 4

        buffer.putShort(bitOffset);         // 4
        buffer.putShort(bitPrecision);      // 4

    }

    @Override
    public DatatypeClass getDatatypeClass() {
            return DatatypeClass.FIXED;
    }

    @Override
    public BitSet getClassBitBytes() {
        return classBitField;
    }

}

