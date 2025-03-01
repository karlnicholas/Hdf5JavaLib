package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.data.FixedPointDataSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Spliterator;
import java.util.function.Consumer;

public class HdfFixedPointSpliterator<T> implements Spliterator<T> {
    private final FileChannel fileChannel;
    private final long sizeForReadBuffer;
    private final long endOffset;
    private long currentOffset;
    private final FixedPointDataSource<T> fixedPointVectorSource;

    public HdfFixedPointSpliterator(FileChannel fileChannel, long startOffset, long endOffset, FixedPointDataSource<T> fixedPointVectorSource) {
        this.fileChannel = fileChannel;
        this.sizeForReadBuffer = fixedPointVectorSource.getSizeForReadBuffer();
        this.currentOffset = startOffset;
        this.endOffset = endOffset;
        this.fixedPointVectorSource = fixedPointVectorSource;
    }

    public HdfFixedPointSpliterator(FileChannel fileChannel, long startOffset, FixedPointDataSource<T> fixedPointVectorSource) {
        this(fileChannel, startOffset,
                startOffset + fixedPointVectorSource.getSizeForReadBuffer() * fixedPointVectorSource.getNumberOfReadsAvailable(),
                fixedPointVectorSource);
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (currentOffset >= endOffset) {
            return false;
        }

        try {
            ByteBuffer buffer = ByteBuffer.allocate((int) sizeForReadBuffer).order(ByteOrder.LITTLE_ENDIAN);
            synchronized (fileChannel) {
                fileChannel.position(currentOffset);
                int totalBytesRead = 0;
                while (totalBytesRead < sizeForReadBuffer) {
                    int bytesRead = fileChannel.read(buffer);
                    if (bytesRead == -1) { // EOF reached
                        if (totalBytesRead == 0) {
                            return false; // No data at all
                        }
                        break; // Accept partial read at EOF
                    }
                    totalBytesRead += bytesRead;
                }
                // Only proceed if we got a full record or are at EOF with some data
                if (totalBytesRead < sizeForReadBuffer && currentOffset + sizeForReadBuffer <= endOffset) {
                    return false; // Incomplete mid-file record
                }
            }
            buffer.flip();
            T dataSource = fixedPointVectorSource.populateFromBuffer(buffer);
            action.accept(dataSource);
            currentOffset += sizeForReadBuffer;
            return true;
        } catch (Exception e) {
            throw new RuntimeException("Error processing HDF data at offset " + currentOffset, e);
        }
    }

    @Override
    public Spliterator<T> trySplit() {
        long remainingRecords = (endOffset - currentOffset) / sizeForReadBuffer;
        if (remainingRecords <= 1) {
            return null;
        }

        long splitSize = remainingRecords / 2;
        long splitOffset = currentOffset + splitSize * sizeForReadBuffer;

        Spliterator<T> newSpliterator = new HdfFixedPointSpliterator<>(
                fileChannel, currentOffset, splitOffset, fixedPointVectorSource
        );
        currentOffset = splitOffset;
        return newSpliterator;
    }

    @Override
    public long estimateSize() {
        return (endOffset - currentOffset) / sizeForReadBuffer;
    }

    @Override
    public int characteristics() {
        return NONNULL | ORDERED | IMMUTABLE | SIZED | SUBSIZED;
    }
}