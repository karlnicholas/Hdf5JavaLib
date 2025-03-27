package org.hdf5javalib.datasource;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.io.IOException;
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
    protected final long sizeForReadBuffer;
    protected final long endOffset;
    protected HdfGlobalHeap globalHeap;

    public AbstractTypedStreamingSource(HdfObjectHeaderPrefixV1 headerPrefixV1, FileChannel fileChannel, long startOffset) {
        this.fileChannel = fileChannel;
        this.startOffset = startOffset;
        this.globalHeap = new HdfGlobalHeap(this::initializeGlobalHeap);
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
        datatype.setGlobalHeap(globalHeap);

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

    private void initializeGlobalHeap(int length, long offset, int index) {
        try {
            fileChannel.position(offset);
            globalHeap.readFromFileChannel(fileChannel, (short) 8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public long getSizeForReadBuffer() {
        return sizeForReadBuffer;
    }

    public long getNumberOfReadsAvailable() {
        return readsAvailable;
    }

    protected void validate2D() {
        if (elementsPerRecord == 1) {
            throw new IllegalStateException("Matrix operations require a 2D dataset (dimensions.length == 2)");
        }
    }

    protected abstract T populateFromBufferRaw(ByteBuffer buffer);
    protected abstract T[] populateRowFromBufferRaw(ByteBuffer buffer); // New method for matrix rows

    public abstract Stream<T> stream();
    public abstract Stream<T> parallelStream();
    public abstract T[][] readAllMatrix() throws IOException;
    public abstract Stream<T[]> streamMatrix();
    public abstract Stream<T[]> parallelStreamMatrix();

    protected class DataClassSpliterator implements Spliterator<T> {
        private final long endOffset;
        private long currentOffset;
        private final boolean asMatrix; // Flag to return T[] instead of T

        public DataClassSpliterator(long startOffset, long endOffset, boolean asMatrix) {
            this.currentOffset = startOffset;
            this.endOffset = endOffset;
            this.asMatrix = asMatrix;
        }

        public DataClassSpliterator(long startOffset, long endOffset) {
            this(startOffset, endOffset, false);
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

            Spliterator<T> newSpliterator = new DataClassSpliterator(currentOffset, splitOffset, asMatrix);
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

        // New Spliterator for matrix rows (T[])
        public Spliterator<T[]> asMatrixSpliterator() {
            return new Spliterator<T[]>() {
                @Override
                public boolean tryAdvance(Consumer<? super T[]> action) {
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
                        T[] row = populateRowFromBufferRaw(buffer);
                        action.accept(row);
                        currentOffset += sizeForReadBuffer;
                        return true;
                    } catch (Exception e) {
                        throw new RuntimeException("Error processing HDF matrix data at offset " + currentOffset, e);
                    }
                }

                @Override
                public Spliterator<T[]> trySplit() {
                    long remainingRecords = (endOffset - currentOffset) / sizeForReadBuffer;
                    if (remainingRecords <= 1) {
                        return null;
                    }

                    long splitSize = remainingRecords / 2;
                    long splitOffset = currentOffset + splitSize * sizeForReadBuffer;

                    Spliterator<T[]> newSpliterator = new DataClassSpliterator(currentOffset, splitOffset, true).asMatrixSpliterator();
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
            };
        }
    }
}