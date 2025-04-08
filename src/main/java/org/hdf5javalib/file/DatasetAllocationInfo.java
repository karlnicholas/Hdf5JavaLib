package org.hdf5javalib.file;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Getter
public class DatasetAllocationInfo {
    @Setter(AccessLevel.PACKAGE) private long headerOffset;
    @Setter(AccessLevel.PACKAGE) private long headerSize;
    @Setter(AccessLevel.PACKAGE) private long dataOffset = -1L;
    @Setter(AccessLevel.PACKAGE) private long dataSize = -1L;
    @Setter(AccessLevel.PACKAGE) private long continuationOffset = -1L;
    @Setter(AccessLevel.PACKAGE) private long continuationSize = -1L;

    DatasetAllocationInfo(long headerOffset, long headerSize) {
        if (headerOffset < 0L) throw new IllegalArgumentException("Header offset cannot be negative.");
        if (headerSize <= 0L) throw new IllegalArgumentException("Initial header size must be positive.");
        this.headerOffset = headerOffset;
        this.headerSize = headerSize;
    }

    @Override
    public String toString() {
        return String.format("DatasetAllocationInfo{header{offset=%d, size=%d}, data{offset=%d, size=%d}, continuation{offset=%d, size=%d}}",
                headerOffset, headerSize, dataOffset, dataSize, continuationOffset, continuationSize);
    }
}