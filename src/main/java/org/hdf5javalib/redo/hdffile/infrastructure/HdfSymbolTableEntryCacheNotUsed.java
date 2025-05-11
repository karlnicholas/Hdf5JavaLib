package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

public class HdfSymbolTableEntryCacheNotUsed implements HdfSymbolTableEntryCache {
    /** The cache type (0 for basic, 1 for additional B-Tree and heap offsets). */
    private final int cacheType = 0;
    public static HdfSymbolTableEntryCache readFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws Exception {
        return new HdfSymbolTableEntryCacheNotUsed();
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
    public int getCacheType() {
        return cacheType;
    }
}
