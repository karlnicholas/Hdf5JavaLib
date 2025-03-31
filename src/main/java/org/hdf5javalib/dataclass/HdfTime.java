package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.TimeDatatype;

import java.nio.ByteBuffer;

public class HdfTime implements HdfData {
    private final byte[] bytes;
    private final TimeDatatype datatype;

    public HdfTime(byte[] bytes, TimeDatatype datatype) {
        this.bytes = bytes.clone();
        this.datatype = datatype;
    }

    public HdfTime(Long value, TimeDatatype datatype) {
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
