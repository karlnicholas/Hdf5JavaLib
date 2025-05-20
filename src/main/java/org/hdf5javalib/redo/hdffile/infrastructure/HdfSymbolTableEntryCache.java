package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.hdffile.dataobjects.HdfObjectHeaderPrefixV1;

import java.nio.ByteBuffer;

public interface HdfSymbolTableEntryCache {
    void writeToBuffer(ByteBuffer buffer);
    @Override
    String toString();
    int getCacheType();
    HdfObjectHeaderPrefixV1 getObjectHeader();
}
