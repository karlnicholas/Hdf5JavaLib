package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.nio.channels.SeekableByteChannel;

public class HdfSymbolTableEntryCacheNotUsed implements HdfSymbolTableEntryCache {
    /** The cache type (0 for basic, 1 for additional B-Tree and heap offsets). */
    private final int cacheType = 0;
    public static HdfSymbolTableEntryCache readFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws Exception {
        return new HdfSymbolTableEntryCacheNotUsed();
    }

}
