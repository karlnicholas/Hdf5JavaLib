package com.github.karlnicholas.hdf5javalib.datatype;

public class FloatingPointMember {
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
