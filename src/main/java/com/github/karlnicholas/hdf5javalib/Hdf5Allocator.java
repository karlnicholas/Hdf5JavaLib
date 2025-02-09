package com.github.karlnicholas.hdf5javalib;

class Hdf5Allocator {
    private long currentOffset;

    public Hdf5Allocator(long startOffset) {
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
