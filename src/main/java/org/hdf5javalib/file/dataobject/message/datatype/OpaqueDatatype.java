package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfOpaque;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class OpaqueDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;            // Size of the opaque data in bytes
    private final String asciiTag;     // NUL-terminated ASCII tag, padded to 8-byte multiple

    private static final Map<Class<?>, HdfConverter<OpaqueDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfOpaque.class, HdfOpaque::new);
        CONVERTERS.put(HdfData.class, HdfOpaque::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes); // Raw bytes access
    }

    public OpaqueDatatype(byte classAndVersion, BitSet classBitField, int size, String asciiTag) {
        int tagLength = getTagLength(classBitField); // Length including NUL, per bit field
        int actualLength = asciiTag.getBytes(StandardCharsets.US_ASCII).length + 1; // String length + NUL
        if (actualLength > tagLength) {
            throw new IllegalArgumentException("ASCII tag length including NUL (" + actualLength +
                    ") exceeds bit field specification (" + tagLength + ")");
        }
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.asciiTag = asciiTag;
    }

    public static OpaqueDatatype parseOpaqueDatatype(byte classAndVersion, BitSet classBitField,
                                                     int size, ByteBuffer buffer) {
        int tagLength = getTagLength(classBitField); // Length including NUL, before padding
        int paddedLength = (tagLength + 7) & ~7;     // Round up to next 8-byte multiple
        byte[] tagBytes = new byte[paddedLength];
        buffer.get(tagBytes);

        // Find NUL terminator and extract string
        int nullIndex = 0;
        while (nullIndex < tagLength && tagBytes[nullIndex] != 0) nullIndex++;
        if (nullIndex >= tagLength) {
            throw new IllegalArgumentException("ASCII tag is not NUL-terminated within specified length");
        }
        String asciiTag = new String(tagBytes, 0, nullIndex, StandardCharsets.US_ASCII);

        return new OpaqueDatatype(classAndVersion, classBitField, size, asciiTag);
    }

    public static BitSet createClassBitField(int tagLength) {
        if (tagLength < 1 || tagLength > 256) { // Includes NUL, so min 1 byte
            throw new IllegalArgumentException("Tag length must be between 1 and 256 bytes");
        }
        BitSet bits = new BitSet(24);
        // Set bits 0-7 for tag length
        for (int i = 0; i < 8; i++) {
            bits.set(i, (tagLength & (1 << i)) != 0);
        }
        // Bits 8-23 reserved as zero
        return bits;
    }

    public static byte createClassAndVersion(int version) {
        if (version != 1) { // Opaque typically uses version 1
            throw new IllegalArgumentException("Opaque Datatype only supports version 1");
        }
        return (byte) ((5 << 4) | version); // Class 5, specified version
    }

    public static int getTagLength(BitSet classBitField) {
        int length = 0;
        for (int i = 0; i < 8; i++) {
            if (classBitField.get(i)) {
                length |= 1 << i;
            }
        }
        return length;
    }

    public static <T> void addConverter(Class<T> clazz, HdfConverter<OpaqueDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<OpaqueDatatype, T> converter = (HdfConverter<OpaqueDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<OpaqueDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    public String toString(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        // Return first 15 bytes as hex, truncate if longer
        StringBuilder sb = new StringBuilder();
        int limit = Math.min(bytes.length, 15);
        for (int i = 0; i < limit; i++) {
            sb.append(String.format("%02X", bytes[i]));
        }
        if (bytes.length > 15) {
            sb.append("... (truncated)");
        }
        return sb.toString();
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.OPAQUE;
    }

    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    @Override
    public short getSizeMessageData() {
        int tagLength = asciiTag.getBytes(StandardCharsets.US_ASCII).length + 1; // Include NUL
        return (short) ((tagLength + 7) & ~7); // Padded to 8-byte multiple
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        byte[] tagBytes = asciiTag.getBytes(StandardCharsets.US_ASCII);
        buffer.put(tagBytes);
        buffer.put((byte) 0); // NUL terminator
        int padding = 8 - ((tagBytes.length + 1) % 8);
        if (padding < 8) { // If not already on 8-byte boundary
            buffer.put(new byte[padding]);
        }
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        // Empty implementation to satisfy interface
    }

    @Override
    public String toString() {
        return "OpaqueDatatype{" +
                "size=" + size +
                ", asciiTag='" + asciiTag + '\'' +
                '}';
    }
}