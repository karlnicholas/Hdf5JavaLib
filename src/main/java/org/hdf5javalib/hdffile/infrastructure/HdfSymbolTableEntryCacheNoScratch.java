package org.hdf5javalib.hdffile.infrastructure;

public class HdfSymbolTableEntryCacheNoScratch implements HdfSymbolTableEntryCache {
    public static final int SYMBOL_TABLE_ENTRY_SCRATCH_SIZE = 16;
    /**
     * The cache type (0 for basic, 1 for additional B-Tree and heap offsets).
     */
    private final int cacheType = 0;

    @Override
    public String toString() {
        return "HdfSymbolTableEntryCacheNoScratch{" + "cacheType=" + cacheType +
                "}";
    }

    @Override
    public int getCacheType() {
        return cacheType;
    }

}
