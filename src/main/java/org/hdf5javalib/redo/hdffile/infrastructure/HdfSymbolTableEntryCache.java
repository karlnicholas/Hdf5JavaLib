package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.nio.ByteBuffer;

public interface HdfSymbolTableEntryCache {
    void writeToBuffer(ByteBuffer buffer);

    @Override
    String toString();

    int getCacheType();

    HdfObjectHeaderPrefix getObjectHeader();
}
