package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.BitFieldDatatype;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class HdfBitField implements HdfData {
    private final byte[] bytes;
    private final BitFieldDatatype datatype;

    /**
     * Constructs an HdfBitField from a byte array and a specified BitFieldDatatype.
     * <p>
     * This constructor initializes the HdfBitField by storing a reference to the provided byte array
     * and associating it with the given datatype. The byte array length must match the size specified by
     * the datatype, or an exception is thrown. The byte array is assumed to be in the correct endianness
     * as defined by the datatype.
     * </p>
     *
     * @param bytes    the byte array containing the bit field data
     * @param datatype the BitFieldDatatype defining the bit precision, offset, padding, and endianness
     * @throws IllegalArgumentException if the byte array length does not match the datatype's size
     * @throws NullPointerException     if either {@code bytes} or {@code datatype} is null
     */
    public HdfBitField(byte[] bytes, BitFieldDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
        if (bytes.length != datatype.getSize()) {
            throw new IllegalArgumentException(
                    String.format("Byte array length (%d) does not match datatype size (%d)",
                            bytes.length, datatype.getSize())
            );
        }
        this.bytes = bytes;
        this.datatype = datatype;
    }

    /**
     * Constructs an HdfBitField from a BitSet and a specified BitFieldDatatype.
     * <p>
     * This constructor initializes the HdfBitField by converting the provided BitSet into a byte array,
     * applying padding as defined by the datatype, and handling endianness. The BitSet is trimmed to the
     * bit precision specified in the datatype, and padding is applied to the low and high bits if necessary.
     * The resulting byte array is stored in little-endian or big-endian format based on the datatype's
     * endianness setting.
     * </p>
     *
     * @param value    the BitSet containing the bit values to be stored
     * @param datatype the BitFieldDatatype defining the bit precision, offset, padding, and endianness
     * @throws NullPointerException if either {@code value} or {@code datatype} is null
     */
    public HdfBitField(BitSet value, BitFieldDatatype datatype) {
        if (value == null || datatype == null) {
            throw new NullPointerException("Value and datatype must not be null");
        }

        this.bytes = new byte[datatype.getSize()];
        int totalBits = datatype.getSize() * 8;

        // Trim BitSet to bitPrecision
        BitSet trimmed = new BitSet(datatype.getBitPrecision());
        for (int i = 0; i < datatype.getBitPrecision() && i < value.length(); i++) {
            if (value.get(i)) {
                trimmed.set(i);
            }
        }

        // Convert to full BitSet with offset
        BitSet fullBitSet = new BitSet(totalBits);
        for (int i = 0; i < datatype.getBitPrecision(); i++) {
            if (trimmed.get(i)) {
                fullBitSet.set(datatype.getBitOffset() + i);
            }
        }

        // Apply padding if bitPrecision is less than totalBits
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

        // Convert BitSet to byte array
        byte[] fullBytes = fullBitSet.toByteArray();
        int copyLength = Math.min(fullBytes.length, this.bytes.length);
        System.arraycopy(fullBytes, 0, this.bytes, 0, copyLength);

        // Adjust for endianness
        if (!datatype.isBigEndian()) {
            reverseInPlace(this.bytes); // Convert to little-endian
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