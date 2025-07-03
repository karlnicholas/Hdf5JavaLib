package org.hdf5javalib.maydo.hdffile.infrastructure;

public class HdfSymbolTableEntryCacheNoScratch implements HdfSymbolTableEntryCache {
    public static final int SYMBOL_TABLE_ENTRY_SCRATCH_SIZE = 16;
    /**
     * The cache type (0 for basic, 1 for additional B-Tree and heap offsets).
     */
    private final int cacheType = 0;

//    @Override
//    public void writeToBuffer(ByteBuffer buffer) {
//        buffer.putInt(cacheType);
//
//        // Write Reserved Field (4 bytes, must be 0)
//        buffer.putInt(0);
//
//        byte[] empty = new byte[SYMBOL_TABLE_ENTRY_SCRATCH_SIZE];
//        buffer.put(empty);
//    }

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
