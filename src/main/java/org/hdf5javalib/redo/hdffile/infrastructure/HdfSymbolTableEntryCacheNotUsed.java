package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class HdfSymbolTableEntryCacheNotUsed implements HdfSymbolTableEntryCache {
    private static final int SYMBOL_TABLE_ENTRY_SCRATCH_SIZE = 16;
    /**
     * The cache type (0 for basic, 1 for additional B-Tree and heap offsets).
     */
    private final int cacheType = 0;
    private final HdfDataSet dataSet;

    public HdfSymbolTableEntryCacheNotUsed(HdfDataFile hdfDataFile, HdfObjectHeaderPrefix objectHeader, String datasetName) {
        dataSet = new HdfDataSet(hdfDataFile, datasetName, objectHeader);
    }

    public static HdfSymbolTableEntryCacheNotUsed readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            HdfObjectHeaderPrefix objectHeader,
            String objectName
    ) throws IOException {
        HdfReadUtils.skipBytes(fileChannel, SYMBOL_TABLE_ENTRY_SCRATCH_SIZE); // Skip 16 bytes for scratch-pad
        return new HdfSymbolTableEntryCacheNotUsed(hdfDataFile, objectHeader, objectName);
    }


    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        byte[] empty = new byte[SYMBOL_TABLE_ENTRY_SCRATCH_SIZE];
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
    public HdfObjectHeaderPrefix getObjectHeader() {
        return dataSet.getDataObjectHeaderPrefix();
    }

    public HdfDataSet getDataSet() {
        return dataSet;
    }
}
