package com.github.karlnicholas.hdf5javalib.message;

import java.nio.ByteBuffer;

public interface HdfMessage {
    void writeToByteBuffer(ByteBuffer buffer, int offsetSize);
}
