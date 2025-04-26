package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.FloatingPointDatatype;

import java.nio.ByteBuffer;

public class HdfFloatPoint implements HdfData {
    private final byte[] bytes;
    private final FloatingPointDatatype datatype;

    /**
     * Constructs an HdfFloatPoint from a byte array and a specified FloatingPointDatatype.
     * <p>
     * This constructor initializes the HdfFloatPoint by storing a reference to the provided byte array
     * and associating it with the given datatype. The byte array is expected to represent a floating-point
     * number formatted according to the datatype's specifications, including precision and endianness.
     * </p>
     *
     * @param bytes    the byte array containing the floating-point data
     * @param datatype the FloatingPointDatatype defining the floating-point structure, size, and format
     * @throws NullPointerException if either {@code bytes} or {@code datatype} is null
     */
    public HdfFloatPoint(byte[] bytes, FloatingPointDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
        this.bytes = bytes.clone();
        this.datatype = datatype;
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
