package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;

/**
 * HDFString. Stored bytes are not null terminated even if null termination is set in classBitField.
 * Length however includes the null terminator if it is defined.
 * Stored bytes only hold bytes, encoded if needed. Null termination is added if needed when bytes are retrieved.
 * Java Strings are not null terminated and are UTF-8 encoded.
 */
public class HdfString implements HdfData {
    private final byte[] bytes;
    private final int length;
    private final BitSet classBitField;

    // Constructor for HDF metadata-based initialization (comprehensive parameters)
    public HdfString(byte[] bytes, BitSet classBitField) {
        this.classBitField = classBitField;
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array cannot be null");
        }

        int zeroLocation = 0;
        for(byte b : bytes) {
            if (b != 0) {
                zeroLocation++;
            } else {
                break;
            }
        }
        this.bytes = Arrays.copyOf(bytes, Math.min(bytes.length, zeroLocation));
        // set length to original byte[] length
        this.length = bytes.length;
    }

    /**
     * nul-padded UTF8 encoded from Java String
     * @param value String
     */
    public HdfString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("String value cannot be null");
        }

        classBitField = StringDatatype.getStringTypeBitSet(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.UTF8);

        this.bytes = value.getBytes(StandardCharsets.UTF_8);
        this.length = bytes.length;
    }

    // Get the string value for application use
    public String getValue() {
        return StringDatatype.CharacterSet.fromBitSet(classBitField) == StringDatatype.CharacterSet.UTF8
                ? new String(bytes, StandardCharsets.UTF_8)
                : new String(bytes, StandardCharsets.US_ASCII);
    }

    // Get the HDF byte[] representation for storage, always returns a copy
    public byte[] getBytes() {
        byte[] copy = new byte[length];
        System.arraycopy(bytes, 0, copy, 0, bytes.length);
        return copy;
    }

    // String representation for debugging and user-friendly output
    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public short getSizeMessageData() {
//        return (short) (StringDatatype.PaddingType.fromBitSet(classBitField) == StringDatatype.PaddingType.NULL_TERMINATE ? length + 1 : length);
        return (short) length;
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(getBytes());
    }
}
