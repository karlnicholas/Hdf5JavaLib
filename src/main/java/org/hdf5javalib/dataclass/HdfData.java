package org.hdf5javalib.dataclass;

import java.nio.ByteBuffer;

public interface HdfData<T> {
    int getSizeMessageData();
    void writeValueToByteBuffer(ByteBuffer buffer);
    T getInstance();
//    short getSize();
}
