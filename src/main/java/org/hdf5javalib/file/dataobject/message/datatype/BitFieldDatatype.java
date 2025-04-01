package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfBitField;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class BitFieldDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitOffset;    // Bit offset of the first significant bit
    private final short bitPrecision; // Number of bits of precision

    private static final Map<Class<?>, HdfConverter<BitFieldDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(BitSet.class, (bytes, dt) -> dt.toBitSet(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfBitField.class, HdfBitField::new);
        CONVERTERS.put(HdfData.class, HdfBitField::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    public BitFieldDatatype(byte classAndVersion, BitSet classBitField, int size, short bitOffset, short bitPrecision) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
    }

    public static BitFieldDatatype parseBitFieldType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();
        return new BitFieldDatatype(classAndVersion, classBitField, size, bitOffset, bitPrecision);
    }

    public static BitSet createClassBitField(boolean bigEndian, int loPadValue, int hiPadValue) {
        if (loPadValue != 0 && loPadValue != 1) throw new IllegalArgumentException("loPadValue must be 0 or 1");
        if (hiPadValue != 0 && hiPadValue != 1) throw new IllegalArgumentException("hiPadValue must be 0 or 1");
        BitSet bits = new BitSet(24);
        bits.set(0, bigEndian);     // Bit 0: Byte Order (0 = little-endian, 1 = big-endian)
        bits.set(1, loPadValue == 1); // Bit 1: Low padding value (0 or 1)
        bits.set(2, hiPadValue == 1); // Bit 2: High padding value (0 or 1)
        // Bits 3-23 reserved as zero
        return bits;
    }

    public static byte createClassAndVersion() {
        return 0x14; // Version 1, Class 4 for BitField
    }

    public static <T> void addConverter(Class<T> clazz, HdfConverter<BitFieldDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<BitFieldDatatype, T> converter = (HdfConverter<BitFieldDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<BitFieldDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    public boolean isBigEndian() {
        return classBitField.get(0);
    }

    public int getLoPadValue() {
        return classBitField.get(1) ? 1 : 0; // Return the padding value (0 or 1)
    }

    public int getHiPadValue() {
        return classBitField.get(2) ? 1 : 0; // Return the padding value (0 or 1)
    }

    public BitSet toBitSet(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length + ") does not match datatype size (" + size + ")");
        }

        // Convert bytes to BitSet with correct byte order
        BitSet fullBitSet = BitSet.valueOf(isBigEndian() ? bytes : reverseBytes(bytes));
        int totalBits = size * 8;

        // Apply padding if bitPrecision < totalBits
        if (bitPrecision < totalBits) {
            int loPadValue = getLoPadValue();
            for (int i = 0; i < bitOffset; i++) {
                fullBitSet.set(i, loPadValue == 1);
            }
            int hiPadValue = getHiPadValue();
            for (int i = bitOffset + bitPrecision; i < totalBits; i++) {
                fullBitSet.set(i, hiPadValue == 1);
            }
        }

        // Extract the significant bits
        BitSet result = new BitSet(bitPrecision);
        for (int i = 0; i < bitPrecision; i++) {
            if (fullBitSet.get(bitOffset + i)) {
                result.set(i);
            }
        }
        return result;
    }

    private byte[] reverseBytes(byte[] array) {
        byte[] reversed = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            reversed[i] = array[array.length - 1 - i];
        }
        return reversed;
    }

    public String toString(byte[] bytes) {
        BitSet bitSet = toBitSet(bytes);
        StringBuilder sb = new StringBuilder();
        for (int i = bitPrecision - 1; i >= 0; i--) {
            sb.append(bitSet.get(i) ? "1" : "0");
        }
        return sb.toString();
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.BITFIELD;
    }

    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    @Override
    public short getSizeMessageData() {
        return 4; // 2 bytes for bitOffset + 2 bytes for bitPrecision
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort(bitOffset);
        buffer.putShort(bitPrecision);
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap grok) {}

    @Override
    public String toString() {
        return "BitFieldDatatype{" +
                "size=" + size +
                ", bitOffset=" + bitOffset +
                ", bitPrecision=" + bitPrecision +
                ", bigEndian=" + isBigEndian() +
                ", loPadValue=" + getLoPadValue() +
                ", hiPadValue=" + getHiPadValue() +
                '}';
    }
}