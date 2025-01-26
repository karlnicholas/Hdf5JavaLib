package com.github.karlnicholas.hdf5javalib.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Spliterator;
import java.util.function.Consumer;

public class HdfSpliterator<T> implements Spliterator<T> {
    private final FileChannel fileChannel;
    private final long recordSize;
    private final long endOffset;
    private long currentOffset;
    private HdfDataSource<T> hdfDataSource;

    public HdfSpliterator(FileChannel fileChannel, long startOffset, long recordSize, long numberOfRecords, HdfDataSource<T> hdfDataSource) {
        this.fileChannel = fileChannel;
        this.recordSize = recordSize;
        this.currentOffset = startOffset;
        this.endOffset = startOffset + recordSize * numberOfRecords;
        this.hdfDataSource = hdfDataSource;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (currentOffset >= endOffset) {
            return false;
        }

        try {
            ByteBuffer buffer = ByteBuffer.allocate((int) recordSize).order(ByteOrder.LITTLE_ENDIAN);
            fileChannel.position(currentOffset);
            int bytesRead = fileChannel.read(buffer);

            if (bytesRead == -1 || bytesRead < recordSize) {
                return false;
            }

            buffer.flip();

            // Use the prototype to populate a new instance
            T dataSource = hdfDataSource.populateFromBuffer(buffer);

            action.accept(dataSource);

            currentOffset += recordSize;
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Error processing HDF data", e);
        }
    }

    @Override
    public Spliterator<T> trySplit() {
        long remainingRecords = (endOffset - currentOffset) / recordSize;
        if (remainingRecords <= 1) {
            return null;
        }

        long midpoint = currentOffset + (remainingRecords / 2) * recordSize;
        Spliterator<T> newSpliterator = new HdfSpliterator<>(
                fileChannel, currentOffset, recordSize, (midpoint - currentOffset) / recordSize, hdfDataSource
        );
        currentOffset = midpoint;
        return newSpliterator;
    }

    @Override
    public long estimateSize() {
        return (endOffset - currentOffset) / recordSize;
    }

    @Override
    public int characteristics() {
        return NONNULL | ORDERED | IMMUTABLE | SIZED | SUBSIZED;
    }
}
