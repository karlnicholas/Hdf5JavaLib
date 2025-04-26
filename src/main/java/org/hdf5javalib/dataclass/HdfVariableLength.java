package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.VariableLengthDatatype;

import java.nio.ByteBuffer;

public class HdfVariableLength implements HdfData {
    private final byte[] bytes;
    private final VariableLengthDatatype datatype;

    /**
     * Constructs an HdfVariableLength from a byte array and a specified VariableLengthDatatype.
     * <p>
     * This constructor initializes the HdfVariableLength by storing a reference to the provided byte
     * array and associating it with the given datatype. The byte array is expected to contain
     * variable-length data formatted according to the datatype's specifications. This constructor is
     * typically used for HDF metadata-based initialization with comprehensive parameters.
     * </p>
     *
     * @param bytes    the byte array containing the variable-length data
     * @param datatype the VariableLengthDatatype defining the data structure and format
     * @throws NullPointerException if either {@code bytes} or {@code datatype} is null
     */
    public HdfVariableLength(byte[] bytes, VariableLengthDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
        this.bytes = bytes;
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
