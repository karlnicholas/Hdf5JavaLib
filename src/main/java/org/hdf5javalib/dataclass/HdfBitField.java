package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.BitFieldDatatype;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class HdfBitField implements HdfData {
    private final byte[] bytes;
    private final BitFieldDatatype datatype;

    public HdfBitField(byte[] bytes, BitFieldDatatype datatype) {
        if (bytes.length != datatype.getSize()) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length + ") does not match datatype size (" + datatype.getSize() + ")");
        }
        this.bytes = bytes.clone();
        this.datatype = datatype;
    }

    public HdfBitField(BitSet value, BitFieldDatatype datatype) {
        this.bytes = new byte[datatype.getSize()];
        int totalBits = datatype.getSize() * 8;

        // Ensure the BitSet is trimmed to bitPrecision
        BitSet trimmed = new BitSet(datatype.getBitPrecision());
        for (int i = 0; i < datatype.getBitPrecision() && i < value.length(); i++) {
            if (value.get(i)) {
                trimmed.set(i);
            }
        }

        // Convert to full BitSet with padding
        BitSet fullBitSet = new BitSet(totalBits);
        for (int i = 0; i < datatype.getBitPrecision(); i++) {
            if (trimmed.get(i)) {
                fullBitSet.set(datatype.getBitOffset() + i);
            }
        }

        // Apply padding if bitPrecision < totalBits
        if (datatype.getBitPrecision() < totalBits) {
            int loPadValue = datatype.getLoPadValue();
            for (int i = 0; i < datatype.getBitOffset(); i++) {
                fullBitSet.set(i, loPadValue == 1);
            }
            int hiPadValue = datatype.getHiPadValue();
            for (int i = datatype.getBitOffset() + datatype.getBitPrecision(); i < totalBits; i++) {
                fullBitSet.set(i, hiPadValue == 1);
            }
        }

        // Convert to bytes
        byte[] fullBytes = fullBitSet.toByteArray();
        int copyLength = Math.min(fullBytes.length, this.bytes.length);
        System.arraycopy(fullBytes, 0, this.bytes, 0, copyLength);
        if (!datatype.isBigEndian()) {
            reverseInPlace(this.bytes); // Convert to little-endian if needed
        }
        this.datatype = datatype;
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public BitSet getValue() {
        return datatype.toBitSet(bytes);
    }

    @Override
    public String toString() {
        return datatype.toString(bytes);
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }

    private void reverseInPlace(byte[] array) {
        for (int i = 0; i < array.length / 2; i++) {
            byte temp = array[i];
            array[i] = array[array.length - 1 - i];
            array[array.length - 1 - i] = temp;
        }
    }
}