package com.github.karlnicholas.hdf5javalib.datatype;

import java.nio.ByteBuffer;

public interface HdfDataType {
    public short getSizeMessageData();

    void writeToByteBuffer(ByteBuffer buffer);
}
