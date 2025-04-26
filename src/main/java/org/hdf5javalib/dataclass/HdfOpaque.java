package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.OpaqueDatatype;

import java.nio.ByteBuffer;

public class HdfOpaque implements HdfData {
    private final byte[] bytes;
    private final OpaqueDatatype datatype;

    /**
     * Constructs an HdfOpaque from a byte array and a specified OpaqueDatatype.
     * <p>
     * This constructor initializes the HdfOpaque by storing a reference to the provided byte array
     * and associating it with the given datatype. The byte array length must match the size specified
     * by the datatype, or an exception is thrown. The byte array is treated as opaque data, with no
     * specific interpretation imposed beyond the datatype's size and format requirements.
     * </p>
     *
     * @param bytes    the byte array containing the opaque data
     * @param datatype the OpaqueDatatype defining the data size and format
     * @throws IllegalArgumentException if the byte array length does not match the datatype's size
     * @throws NullPointerException     if either {@code bytes} or {@code datatype} is null
     */
    public HdfOpaque(byte[] bytes, OpaqueDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
        if (bytes.length != datatype.getSize()) {
            throw new IllegalArgumentException(
                    String.format("Byte array length (%d) does not match datatype size (%d)",
                            bytes.length, datatype.getSize())
            );
        }
        this.bytes = bytes;
        this.datatype = datatype;
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    @Override
    public String toString() {
        return datatype.getInstance(String.class, bytes);
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }
}