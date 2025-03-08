package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.nio.ByteBuffer;

/**
 * HDFString. Stored bytes are not null terminated even if null termination is set in classBitField.
 * Length however includes the null terminator if it is defined.
 * Stored bytes only hold bytes, encoded if needed. Null termination is added if needed when bytes are retrieved.
 * Java Strings are not null terminated and are UTF-8 encoded.
 */
public class HdfString<T> implements HdfData<T> {
    private final Class<T> clazz;
    private final byte[] bytes;
    private final StringDatatype datatype;

    // Constructor for HDF metadata-based initialization (comprehensive parameters)
    public HdfString(Class<T> clazz, byte[] bytes, StringDatatype datatype) {
        this.clazz = clazz;
        this.bytes = bytes.clone();
        this.datatype = datatype;
    }

    /**
     * nul-padded UTF8 encoded from Java String
     * @param value String
     */
    public HdfString(String value, StringDatatype datatype) {
        this((Class<T>) String.class, value.getBytes(), datatype);
    }

    // Get the HDF byte[] representation for storage, always returns a copy
    public byte[] getBytes() {
        byte[] copy = new byte[datatype.getSize()];
        System.arraycopy(bytes, 0, copy, 0, bytes.length);
        return copy;
    }

    // String representation for debugging and user-friendly output
    @Override
    public String toString() {
        return getInstance().toString();
    }

    @Override
    public int getSizeMessageData() {
//        return (short) (StringDatatype.PaddingType.fromBitSet(classBitField) == StringDatatype.PaddingType.NULL_TERMINATE ? length + 1 : length);
        return datatype.getSize();
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(getBytes());
    }

    @Override
    public T getInstance() {
        return datatype.getInstance(clazz, bytes);
    }
}
