package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.OpaqueDatatype;

import java.nio.ByteBuffer;

public class HdfOpaque implements HdfData {
    private final byte[] bytes;
    private final OpaqueDatatype datatype;

    public HdfOpaque(byte[] bytes, OpaqueDatatype datatype) {
        if (bytes.length != datatype.getSize()) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + datatype.getSize() + ")");
        }
        this.bytes = bytes.clone();
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