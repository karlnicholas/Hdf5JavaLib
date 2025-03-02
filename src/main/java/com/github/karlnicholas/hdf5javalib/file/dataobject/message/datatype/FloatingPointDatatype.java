package com.github.karlnicholas.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.BitSet;

@Getter
public class FloatingPointDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final int exponentBits;
    private final int mantissaBits;
    private final boolean bigEndian;

    public FloatingPointDatatype(byte classAndVersion, BitSet classBitField, int size, int exponentBits, int mantissaBits, boolean bigEndian) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.exponentBits = exponentBits;
        this.mantissaBits = mantissaBits;
        this.bigEndian = bigEndian;
    }

    public static FloatingPointDatatype parseFloatingPointType(byte version, BitSet classBitField, int size, ByteBuffer buffer) {
        boolean bigEndian = classBitField.get(0);
        int exponentBits = buffer.getInt();
        int mantissaBits = buffer.getInt();
        return new FloatingPointDatatype(version, classBitField, size, exponentBits, mantissaBits, bigEndian);
    }

    public Object getInstance() {
        return new Object();
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.FLOAT;
    }

    @Override
    public short getSizeMessageData() {
        return (short) size;
    }

    @Override
    public String toString() {
        return "FloatingPointDatatype{" +
                "size=" + size +
                ", exponentBits=" + exponentBits +
                ", mantissaBits=" + mantissaBits +
                ", bigEndian=" + bigEndian +
                '}';
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putInt(exponentBits);
        buffer.putInt(mantissaBits);
    }

}
