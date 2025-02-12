package com.github.karlnicholas.hdf5javalib.datatype;

import java.nio.ByteBuffer;

public interface HdfDataType {
    short getSizeMessageData();
    void writeDefinitionToByteBuffer(ByteBuffer buffer);
    void writeValueToByteBuffer(ByteBuffer buffer);
}
