package com.github.karlnicholas.hdf5javalib.data;

import java.nio.ByteBuffer;

public interface HdfData {
    short getSizeMessageData();
    void writeValueToByteBuffer(ByteBuffer buffer);
//    short getSize();
}
