package org.hdf5javalib.maydo.hdffile.infrastructure;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.utils.HdfReadUtils;

public class HdfSymbolTableEntryCacheWithScratch implements HdfSymbolTableEntryCache {
    /**
     * The cache type (0 for basic, 1 for additional B-Tree and heap offsets).
     */
    private final int cacheType = 1;
    private final HdfFixedPoint bTreeAddress;
    private final HdfFixedPoint localHeapAddress;


    public HdfSymbolTableEntryCacheWithScratch(HdfFixedPoint bTreeAddress, HdfFixedPoint localHeapAddress) {
        this.bTreeAddress = bTreeAddress;
        this.localHeapAddress = localHeapAddress;
    }

    public HdfFixedPoint getbTreeAddress() {
        return bTreeAddress;
    }

    public HdfFixedPoint getLocalHeapAddress() {
        return localHeapAddress;
    }

    /**
     * The offset of the B-Tree for cache type 1 entries (null for cache type 0).
     */

//    @Override
//    public void writeToBuffer(ByteBuffer buffer) {
//        buffer.putInt(cacheType);
//
//        // Write Reserved Field (4 bytes, must be 0)
//        buffer.putInt(0);
//
////        writeFixedPointToBuffer(buffer, group.getBTree().getAllocationRecord().getOffset());
////        writeFixedPointToBuffer(buffer, group.getLocalHeap().getAllocationRecord().getOffset());
//
//    }

    @Override
    public String toString() {
        return "HdfSymbolTableEntryCacheWithScratch{" + "cacheType=" + cacheType +
                "}";
    }

    @Override
    public int getCacheType() {
        return cacheType;
    }

}
