package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.EnumDatatype;

import java.nio.ByteBuffer;

public class HdfEnum implements HdfData {
    private final byte[] bytes;
    private final EnumDatatype datatype;

    /**
     * Constructs an HdfEnum from a byte array and a specified EnumDatatype.
     * <p>
     * This constructor initializes the HdfEnum by storing a reference to the provided byte array
     * and associating it with the given datatype. The byte array length must match the size specified
     * by the datatype, or an exception is thrown. The byte array is expected to represent an enumeration
     * value formatted according to the datatype's specifications, including any endianness requirements.
     * Note that the byte array is not cloned, so modifications to the input array may affect the
     * HdfEnum's data.
     * </p>
     *
     * @param bytes    the byte array containing the enumeration data
     * @param datatype the EnumDatatype defining the enumeration structure, size, and format
     * @throws IllegalArgumentException if the byte array length does not match the datatype's size
     * @throws NullPointerException     if either {@code bytes} or {@code datatype} is null
     */
    public HdfEnum(byte[] bytes, EnumDatatype datatype) {
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