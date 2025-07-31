package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;

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
