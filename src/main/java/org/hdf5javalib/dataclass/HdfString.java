package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.nio.ByteBuffer;

/**
 * HDFString. Stored bytes are not null terminated even if null termination is set in classBitField.
 * Length however includes the null terminator if it is defined.
 * Stored bytes only hold bytes, encoded if needed. Null termination is added if needed when bytes are retrieved.
 * Java Strings are not null terminated and are UTF-8 encoded.
 */
public class HdfString implements HdfData {
    private final byte[] bytes;
    private final StringDatatype datatype;

    // Constructor for HDF metadata-based initialization (comprehensive parameters)
    public HdfString(byte[] bytes, StringDatatype datatype) {
        this.bytes = bytes.clone();
        this.datatype = datatype;
    }

    /**
     * nul-padded UTF8 encoded from Java String
     * @param value String
     */
    public HdfString(String value, StringDatatype datatype) {
        this(value.getBytes(), datatype);
    }

    // Get the HDF byte[] representation for storage, always returns a copy
    public byte[] getBytes() {
        byte[] copy = new byte[getSizeMessageData()];
        System.arraycopy(bytes, 0, copy, 0, bytes.length);
        return copy;
    }

    // String representation for debugging and user-friendly output
    @Override
    public String toString() {
        return datatype.getInstance(String.class, bytes);
    }

    @Override
    public int getSizeMessageData() {
        return (short) (datatype.getPaddingType() == StringDatatype.PaddingType.NULL_TERMINATE ? datatype.getSize() + 1 : datatype.getSize());
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(getBytes());
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }

}
