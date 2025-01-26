package com.github.karlnicholas.hdf5javalib.datatype;

public class FixedPointMember {
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
