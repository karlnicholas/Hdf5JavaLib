package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.TimeDatatype;

import java.nio.ByteBuffer;

public class HdfTime implements HdfData {
    private final byte[] bytes;
    private final TimeDatatype datatype;

    /**
     * Constructs an HdfTime from a byte array and a specified TimeDatatype.
     * <p>
     * This constructor initializes the HdfTime by storing a reference to the provided byte array
     * and associating it with the given datatype. The byte array is expected to represent a time
     * value formatted according to the datatype's specifications, such as size and encoding.
     * </p>
     *
     * @param bytes    the byte array containing the time data
     * @param datatype the TimeDatatype defining the time structure, size, and format
     * @throws NullPointerException if either {@code bytes} or {@code datatype} is null
     */
    public HdfTime(byte[] bytes, TimeDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
        this.bytes = bytes;
        this.datatype = datatype;
    }

    /**
     * Constructs an HdfTime from a Long value and a specified TimeDatatype.
     * <p>
     * This constructor initializes the HdfTime by converting the provided Long value into a byte array
     * of the size specified by the datatype, using the default byte order. The resulting byte array is
     * then associated with the given datatype to represent a time value.
     * </p>
     *
     * @param value    the Long value representing the time data
     * @param datatype the TimeDatatype defining the time structure, size, and format
     * @throws NullPointerException if either {@code value} or {@code datatype} is null
     */
    public HdfTime(Long value, TimeDatatype datatype) {
        if (value == null || datatype == null) {
            throw new NullPointerException("Value and datatype must not be null");
        }
        this.bytes = new byte[datatype.getSize()];
        ByteBuffer.wrap(this.bytes).putLong(value);
        this.datatype = datatype;
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public long getValue() {
        return datatype.toLong(bytes);
    }

    @Override
    public String toString() {
        return Long.toString(getValue());
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
