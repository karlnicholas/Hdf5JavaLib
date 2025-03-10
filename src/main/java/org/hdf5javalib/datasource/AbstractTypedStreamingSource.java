package org.hdf5javalib.datasource;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Abstract base class for reading fixed-point data from HDF5 datasets.
 * Provides common metadata parsing, FileChannel handling, and streaming support for derived classes.
 * Subclasses must implement specific data population logic for typed or raw data access.
 *
 * @param <T> the type of object returned by streaming or bulk reading operations
 */
public abstract class AbstractTypedStreamingSource<T> {
    protected final FileChannel fileChannel;
    protected final long startOffset;
    protected final int recordSize;
    protected final int readsAvailable;
    protected final int elementsPerRecord;
    protected final HdfDatatype datatype;
    protected final int scale;
    protected final long sizeForReadBuffer;
    protected final long endOffset;

    public AbstractTypedStreamingSource(HdfObjectHeaderPrefixV1 headerPrefixV1, int scale, FileChannel fileChannel, long startOffset) {
        this.fileChannel = fileChannel;
        this.startOffset = startOffset;
        this.scale = scale;

        this.recordSize = headerPrefixV1.findMessageByType(DatatypeMessage.class)
                .orElseThrow(() -> new IllegalStateException("DatatypeMessage not found"))
                .getHdfDatatype()
                .getSize();

        HdfFixedPoint[] dimensions = headerPrefixV1.findMessageByType(DataspaceMessage.class)
                .orElseThrow(() -> new IllegalStateException("DataspaceMessage not found"))
                .getDimensions();

        this.readsAvailable = dimensions[0].getInstance(Long.class).intValue();
        this.datatype = headerPrefixV1.findMessageByType(DatatypeMessage.class)
                .orElseThrow()
                .getHdfDatatype();

        if (dimensions.length == 1) {
            this.elementsPerRecord = 1;
        } else if (dimensions.length == 2) {
            this.elementsPerRecord = dimensions[1].getInstance(Long.class).intValue();
        } else {
            throw new IllegalArgumentException("Unsupported dimensionality: " + dimensions.length);
        }

        this.sizeForReadBuffer = (long) recordSize * elementsPerRecord;
        this.endOffset = fileChannel != null ? startOffset + sizeForReadBuffer * readsAvailable : 0;
    }

    public long getSizeForReadBuffer() {
        return sizeForReadBuffer;
    }

    public long getNumberOfReadsAvailable() {
        return readsAvailable;
    }

    protected abstract T populateFromBufferRaw(ByteBuffer buffer);

    public abstract Stream<T> stream();

    public abstract Stream<T> parallelStream();

    protected class DataClassSpliterator implements Spliterator<T> {
        private final long endOffset;
        private long currentOffset;

        public DataClassSpliterator(long startOffset, long endOffset) {
            this.currentOffset = startOffset;
            this.endOffset = endOffset;
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
                        if (bytesRead == -1) {
                            if (totalBytesRead == 0) {
                                return false;
                            }
                            break;
                        }
                        totalBytesRead += bytesRead;
                    }
                    if (totalBytesRead < sizeForReadBuffer && currentOffset + sizeForReadBuffer <= endOffset) {
                        return false;
                    }
                }
                buffer.flip();
                T dataSource = populateFromBufferRaw(buffer);
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

            Spliterator<T> newSpliterator = new DataClassSpliterator(currentOffset, splitOffset);
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
}