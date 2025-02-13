package com.github.karlnicholas.hdf5javalib.file;

public class HdfAllocator {
    private long currentOffset;

    public HdfAllocator(long startOffset) {
        this.currentOffset = startOffset;
    }

    public long allocate(long size, long alignment) {
        // Align offset to the required boundary
        long alignedOffset = (currentOffset + alignment - 1) & ~(alignment - 1);
        currentOffset = alignedOffset + size;
        return alignedOffset;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }
}
