package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfFloatPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        if (clazz.isAssignableFrom(Double.class)) {  // Can accept Double
            return clazz.cast(toDouble(bytes));
        } else if (clazz.isAssignableFrom(Float.class)) {  // Can accept Float
            return clazz.cast(toFloat(bytes));
        } else if (clazz.isAssignableFrom(String.class)) {  // Can accept Float
            return clazz.cast(toDouble(bytes).toString());
        } else if (clazz.isAssignableFrom(HdfFloatPoint.class)) {  // Can accept Float
            return clazz.cast(new HdfFloatPoint(bytes, this));
        } else {
            throw new UnsupportedOperationException("Unknown type: " + clazz);
        }
    }

    @Override
    public <T> T getInstance(Class<T> clazz, ByteBuffer buffer) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return getInstance(clazz, bytes);
    }

    // Convert buffer to Float
    public Float toFloat(byte[] bytes) {
        double value = toDoubleValue(bytes);
        return (float) value; // Cast to float, may lose precision if size > 4 bytes
    }

    // Convert buffer to Double
    public Double toDouble(byte[] bytes) {
        return toDoubleValue(bytes);
    }

    private double toDoubleValue(byte[] buffer) {
        if (buffer.length != size) {
            throw new IllegalArgumentException("Buffer size (" + buffer.length + ") must match datatype size (" + size + ")");
        }

        // Determine byte order from classBitField
        ByteOrder order = getByteOrder();
        ByteBuffer bb = ByteBuffer.wrap(buffer).order(order);

        // Convert buffer to long (up to 64 bits) for bit manipulation
        long bits = 0;
        if (size <= 4) {
            bits = bb.getInt() & 0xFFFFFFFFL; // 32-bit unsigned
        } else if (size <= 8) {
            bits = bb.getLong();
        } else {
            throw new UnsupportedOperationException("Size > 8 bytes not supported");
        }

        // Shift bits to align with datatype.getBitOffset()
        bits >>>= bitOffset; // Unsigned right shift to discard lower bits

        // Extract sign bit (from signLocation in classBitField bits 8-15)
        int signLocation = getSignLocation();
        long signMask = 1L << signLocation;
        int sign = (bits & signMask) != 0 ? -1 : 1;

        // Extract exponent
        long exponentMask = (1L << exponentSize) - 1;
        long rawExponent = (bits >>> exponentLocation) & exponentMask;
        int exponent = (int) (rawExponent - exponentBias);

        // Extract mantissa
        long mantissaMask = (1L << mantissaSize) - 1;
        long mantissa = (bits >>> mantissaLocation) & mantissaMask;

        // Assume normalized number (implied leading 1, common in HDF5 float spec)
        double mantissaValue = 1.0 + (double) mantissa / (1L << mantissaSize); // Normalize mantissa

        // Combine: sign * mantissa * 2^exponent
        double value = sign * mantissaValue * Math.pow(2, exponent);

        return value;
    }

    private ByteOrder getByteOrder() {
        boolean bit0 = classBitField.get(0);
        boolean bit6 = classBitField.get(6);
        if (!bit6 && !bit0) return ByteOrder.LITTLE_ENDIAN;
        if (!bit6 && bit0) return ByteOrder.BIG_ENDIAN;
        if (bit6 && !bit0) throw new IllegalArgumentException("Reserved byte order");
        if (bit6 && bit0) throw new UnsupportedOperationException("VAX-endian not supported");
        return ByteOrder.LITTLE_ENDIAN; // Default, should never reach here
    }

    private int getSignLocation() {
        int signLoc = 0;
        for (int i = 8; i <= 15; i++) {
            if (classBitField.get(i)) {
                signLoc |= (1 << (i - 8));
            }
        }
        return signLoc;
    }
//    @Override
//    public HdfData getInstance(ByteBuffer buffer) {
//        byte[] bytes = new byte[size];
//        buffer.get(bytes);
//        return new HdfFloatPoint(bytes, classBitField, size, bitOffset, bitPrecision, exponentLocation, exponentSize, mantissaLocation, mantissaSize, exponentBias);
//    }

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
