package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataSet;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class HdfSymbolTableEntryCacheNotUsed implements HdfSymbolTableEntryCache {
    /** The cache type (0 for basic, 1 for additional B-Tree and heap offsets). */
    private final int cacheType = 0;
    private final HdfDataSet dataSet;
    public HdfSymbolTableEntryCacheNotUsed(HdfDataFile hdfDataFile, HdfObjectHeaderPrefixV1 objectHeader, String datasetName) {
        dataSet = new HdfDataSet(hdfDataFile, datasetName, objectHeader);
    }

    public static HdfSymbolTableEntryCacheNotUsed readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            HdfObjectHeaderPrefixV1 objectHeader,
            String objectName
    ) throws IOException {
        HdfReadUtils.skipBytes(fileChannel, 16); // Skip 16 bytes for scratch-pad
        return new HdfSymbolTableEntryCacheNotUsed(hdfDataFile, objectHeader, objectName);
    }


    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        byte[] empty = new byte[16];
        buffer.put(empty);
    }

    @Override
    public String toString() {
        return "HdfSymbolTableEntryCacheNotUsed{" + "cacheType=" + cacheType +
                ", dataSet=" + dataSet +
                "}";
    }

    @Override
    public int getCacheType() {
        return cacheType;
    }

    @Override
    public HdfObjectHeaderPrefixV1 getObjectHeader() {
        return dataSet.getDataObjectHeaderPrefix();
    }

    public HdfDataSet getDataSet() {
        return dataSet;
    }
}
