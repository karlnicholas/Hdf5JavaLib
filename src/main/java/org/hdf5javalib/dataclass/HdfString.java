package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.nio.ByteBuffer;

/**
 * HDFString. Stored bytes are not null terminated even if null termination is set in classBitField.
 * The length in the StringDatatype message needs to be length + 1 if the string is defined as
 * null-terminated and the string is not null terminated. There is not a lot of fancy logic here, the app
 * needs to sort out what it expects. Probably need some utils or factory for this.
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
        return datatype.getWorkingBytes(bytes);
    }

    // String representation for debugging and user-friendly output
    @Override
    public String toString() {
        return datatype.getInstance(String.class, bytes);
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
