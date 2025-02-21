package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.data.FixedPointDataSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Spliterator;
import java.util.function.Consumer;

public class HdfFixedPointDatatypeSpliterator<T> implements Spliterator<T> {
    private final FileChannel fileChannel;
    private final long recordSize;
    private final long endOffset;
    private long currentOffset;
    private final FixedPointDataSource<T> fixedPointDataSource;

    public HdfFixedPointDatatypeSpliterator(FileChannel fileChannel, long startOffset, long recordSize, long numberOfRecords, FixedPointDataSource<T> fixedPointDataSource) {
        this.fileChannel = fileChannel;
        this.recordSize = recordSize;
        this.currentOffset = startOffset;
        this.endOffset = startOffset + recordSize * numberOfRecords;
        this.fixedPointDataSource = fixedPointDataSource;
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
            T dataSource = fixedPointDataSource.populateFromBuffer(buffer);

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
        Spliterator<T> newSpliterator = new HdfFixedPointDatatypeSpliterator<>(
                fileChannel, currentOffset, recordSize, (midpoint - currentOffset) / recordSize, fixedPointDataSource
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
