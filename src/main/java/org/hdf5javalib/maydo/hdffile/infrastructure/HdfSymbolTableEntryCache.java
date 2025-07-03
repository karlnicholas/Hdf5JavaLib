package org.hdf5javalib.maydo.hdffile.infrastructure;

import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;

import java.nio.ByteBuffer;

public interface HdfSymbolTableEntryCache {
//    private final HdfObjectHeaderPrefix objectHeader;
//
//    protected HdfSymbolTableEntryCache(HdfObjectHeaderPrefix objectHeader) {
//        this.objectHeader = objectHeader;
//    }
//
//    public HdfObjectHeaderPrefix getObjectHeader() {
//        return objectHeader;
//    }
//
//    public abstract void writeToBuffer(ByteBuffer buffer);
//
//    @Override
//    public abstract String toString();

    int getCacheType();

}
