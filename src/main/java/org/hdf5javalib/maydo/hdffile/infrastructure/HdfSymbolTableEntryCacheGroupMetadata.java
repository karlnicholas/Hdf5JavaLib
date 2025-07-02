package org.hdf5javalib.maydo.hdffile.infrastructure;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdfjava.HdfBTree;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.maydo.hdfjava.HdfGroup;
import org.hdf5javalib.maydo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class HdfSymbolTableEntryCacheGroupMetadata extends HdfSymbolTableEntryCache {
    /**
     * The cache type (0 for basic, 1 for additional B-Tree and heap offsets).
     */
    private final int cacheType = 1;

    public HdfSymbolTableEntryCacheGroupMetadata(HdfObjectHeaderPrefix objectHeader) {
        super(objectHeader);
    }

    /**
     * The offset of the B-Tree for cache type 1 entries (null for cache type 0).
     */

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

//        writeFixedPointToBuffer(buffer, group.getBTree().getAllocationRecord().getOffset());
//        writeFixedPointToBuffer(buffer, group.getLocalHeap().getAllocationRecord().getOffset());

    }

    @Override
    public String toString() {
        return "HdfSymbolTableEntryCacheGroupMetadata{" + "cacheType=" + cacheType +
                "}";
    }

    @Override
    public int getCacheType() {
        return cacheType;
    }

}
