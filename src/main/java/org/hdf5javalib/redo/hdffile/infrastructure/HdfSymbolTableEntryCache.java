package org.hdf5javalib.redo.hdffile.infrastructure;

import java.nio.ByteBuffer;

public interface HdfSymbolTableEntryCache {
    void writeToBuffer(ByteBuffer buffer);
}
