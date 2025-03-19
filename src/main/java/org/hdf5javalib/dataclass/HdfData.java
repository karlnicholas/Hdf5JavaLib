package org.hdf5javalib.dataclass;

import java.nio.ByteBuffer;

public interface HdfData {
    void writeValueToByteBuffer(ByteBuffer buffer);
    <T> T getInstance(Class<T> clazz);
}
