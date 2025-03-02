package org.hdf5javalib.dataclass;

import java.nio.ByteBuffer;

public interface HdfData {
    short getSizeMessageData();
    void writeValueToByteBuffer(ByteBuffer buffer);
//    short getSize();
}
