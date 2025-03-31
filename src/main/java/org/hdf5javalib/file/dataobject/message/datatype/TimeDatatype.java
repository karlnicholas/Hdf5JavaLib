package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfTime;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class TimeDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitPrecision;

    private static final Map<Class<?>, HdfConverter<TimeDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(Long.class, (bytes, dt) -> dt.toLong(bytes));
        CONVERTERS.put(BigInteger.class, (bytes, dt) -> dt.toBigInteger(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfTime.class, HdfTime::new);
        CONVERTERS.put(HdfData.class, HdfTime::new);
    }

    public TimeDatatype(byte classAndVersion, BitSet classBitField, int size, short bitPrecision) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitPrecision = bitPrecision;
    }

    public static TimeDatatype parseTimeType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        short bitPrecision = buffer.getShort();
        return new TimeDatatype(classAndVersion, classBitField, size, bitPrecision);
    }

    public static BitSet createClassBitField(boolean bigEndian) {
        BitSet bits = new BitSet(24);
        bits.set(0, bigEndian);
        return bits;
    }

    public static byte createClassAndVersion() {
        return 0x12;
    }

    public static <T> void addConverter(Class<T> clazz, HdfConverter<TimeDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<TimeDatatype, T> converter = (HdfConverter<TimeDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<TimeDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    public boolean isBigEndian() {
        return classBitField.get(0);
    }

    public Long toLong(byte[] bytes) {
        if (bytes.length > 8) {
            throw new IllegalArgumentException("Cannot convert more than 8 bytes to a long.");
        }

        long result = 0;
        if (isBigEndian()) {
            for (int i = 0; i < bytes.length; i++) {
                result <<= 8;
                result |= (bytes[i] & 0xFFL);
            }
        } else {
            for (int i = bytes.length - 1; i >= 0; i--) {
                result <<= 8;
                result |= (bytes[i] & 0xFFL);
            }
        }

        // Sign extension
        int shift = (8 - bytes.length) * 8;
        return (result << shift) >> shift;
    }

    public BigInteger toBigInteger(byte[] bytes) {
        byte[] copy = bytes.clone();
        if (!isBigEndian()) {
            reverseInPlace(copy);
        }
        return new BigInteger(copy);
    }

    private void reverseInPlace(byte[] array) {
        for (int i = 0; i < array.length / 2; i++) {
            byte temp = array[i];
            array[i] = array[array.length - 1 - i];
            array[array.length - 1 - i] = temp;
        }
    }

    public String toString(byte[] bytes) {
        return toLong(bytes).toString();
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.TIME;
    }

    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    @Override
    public short getSizeMessageData() {
        return 2; // Only bitPrecision
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort(bitPrecision);
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap grok) {}

    @Override
    public String toString() {
        return "TimeDatatype{" +
                "size=" + size +
                ", bitPrecision=" + bitPrecision +
                '}';
    }
}
