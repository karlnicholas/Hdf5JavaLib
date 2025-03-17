package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.VariableLengthDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * HDFString. Stored bytes are not null terminated even if null termination is set in classBitField.
 * The length in the StringDatatype message needs to be length + 1 if the string is defined as
 * null-terminated and the string is not null terminated. There is not a lot of fancy logic here, the app
 * needs to sort out what it expects. Probably need some utils or factory for this.
 * Stored bytes only hold bytes, encoded if needed. Null termination is added if needed when bytes are retrieved.
 * Java Strings are not null terminated and are UTF-8 encoded.
 */
public class HdfVariableLength implements HdfData {
    private final byte[] bytes;
    private final VariableLengthDatatype datatype;

    // Constructor for HDF metadata-based initialization (comprehensive parameters)
    public HdfVariableLength(byte[] bytes, VariableLengthDatatype datatype) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int length = buffer.getInt();
        long offset = buffer.getLong();
        int index = buffer.getInt();
        this.bytes = datatype.getGlobalHeap().getDataBytes(length, offset, index);
//        this.bytes = bytes.clone();
        this.datatype = datatype;
    }

    /**
     * nul-padded UTF8 encoded from Java String
     * @param value String
     */
    public HdfVariableLength(String value, VariableLengthDatatype datatype) {
        this.bytes = value.getBytes();
        this.datatype = datatype;
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
        return datatype.getSize();
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
