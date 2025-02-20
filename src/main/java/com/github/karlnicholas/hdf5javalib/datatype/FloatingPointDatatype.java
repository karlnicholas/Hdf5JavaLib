package com.github.karlnicholas.hdf5javalib.datatype;

import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
public class FloatingPointDatatype implements HdfDatatype {
    private final byte version;
    private final int size;
    private final int exponentBits;
    private final int mantissaBits;
    private final boolean bigEndian;

    public FloatingPointDatatype(byte version, int size, int exponentBits, int mantissaBits, boolean bigEndian) {
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
        return "FloatingPointDatatype{" +
                "size=" + size +
                ", exponentBits=" + exponentBits +
                ", mantissaBits=" + mantissaBits +
                ", bigEndian=" + bigEndian +
                '}';
    }

    @Override
    public short getSizeMessageData() {
        return (short)size;
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putInt(exponentBits);
        buffer.putInt(mantissaBits);
    }

}
