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
            HdfObjectHeaderPrefixV1 objectHeader
    ) throws IOException {
        HdfReadUtils.skipBytes(fileChannel, 16); // Skip 16 bytes for scratch-pad
//        fileChannel.position(objectHeader.getOffset().getInstance(Long.class));
//        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromSeekableByteChannel(fileChannel, hdfDataFile);
        return new HdfSymbolTableEntryCacheNotUsed(hdfDataFile, objectHeader, "datasetName");
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
        StringBuilder sb = new StringBuilder("HdfSymbolTableEntryCacheNotUsed{");
        sb.append("cacheType=").append(cacheType);
        sb.append(", dataSet=").append(dataSet);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public HdfObjectHeaderPrefixV1 getObjectHeader() {
        return dataSet.getDataObjectHeaderPrefix();
    }
}
