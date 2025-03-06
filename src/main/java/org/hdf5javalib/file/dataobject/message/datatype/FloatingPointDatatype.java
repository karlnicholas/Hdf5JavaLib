package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFloatPoint;

import java.nio.ByteBuffer;
import java.util.BitSet;

@Getter
public class FloatingPointDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitOffset;
    private final short bitPrecision;
    private final byte exponentLocation;
    private final byte exponentSize;
    private final byte mantissaLocation;
    private final byte mantissaSize;
    private final int exponentBias;

    public FloatingPointDatatype(byte classAndVersion, BitSet classBitField, int size, short bitOffset, short bitPrecision, byte exponentLocation, byte exponentSize, byte mantissaLocation, byte mantissaSize, int exponentBias) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
        this.exponentLocation = exponentLocation;
        this.exponentSize = exponentSize;
        this.mantissaLocation = mantissaLocation;
        this.mantissaSize = mantissaSize;
        this.exponentBias = exponentBias;
    }


    public static FloatingPointDatatype parseFloatingPointType(byte version, BitSet classBitField, int size, ByteBuffer buffer) {
        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();
        byte exponentLocation = buffer.get();
        byte exponentSize = buffer.get();
        byte mantissaLocation = buffer.get();
        byte mantissaSize = buffer.get();
        int exponentBias = buffer.getInt();
        return new FloatingPointDatatype(version, classBitField, size,bitOffset, bitPrecision, exponentLocation, exponentSize, mantissaLocation, mantissaSize, exponentBias);
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
    public HdfData getInstance(ByteBuffer buffer) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new HdfFloatPoint(bytes, classBitField, size, bitOffset, bitPrecision, exponentLocation, exponentSize, mantissaLocation, mantissaSize, exponentBias);
    }

    @Override
    public String toString() {
        return "FloatingPointDatatype{" +
                "size=" + size +
                ", bitOffset=" + bitOffset +
                ", bitPrecision=" + bitPrecision +
                ", exponentLocation=" + exponentLocation +
                ", exponentSize=" + exponentSize +
                ", mantissaLocation=" + mantissaLocation +
                ", mantissaSize=" + mantissaSize +
                ", exponentBias=" + exponentBias +
                '}';
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort(bitOffset);
        buffer.putShort(bitPrecision);
        buffer.put(exponentLocation);
        buffer.put(exponentSize);
        buffer.put(mantissaLocation);
        buffer.put(mantissaSize);
        buffer.putInt(exponentBias);
    }

}
