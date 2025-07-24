package org.hdf5javalib.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ByteBufferDeflater {
    ByteBuffer deflate(ByteBuffer input) throws IOException;
}
