package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.VariableLengthDatatype;

import java.nio.ByteBuffer;

public class HdfVariableLength implements HdfData {
    private final byte[] bytes;
    private final VariableLengthDatatype datatype;

    // Constructor for HDF metadata-based initialization (comprehensive parameters)
    public HdfVariableLength(byte[] bytes, VariableLengthDatatype datatype) {
        this.bytes = bytes.clone();
        this.datatype = datatype;
    }

    // Get the HDF byte[] representation for storage, always returns a copy
    public byte[] getBytes() {
        byte[] copy = new byte[ datatype.getSize()];
        System.arraycopy(bytes, 0, copy, 0, bytes.length);
        return copy;
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
