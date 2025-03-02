package com.github.karlnicholas.hdf5javalib.data;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.DataspaceMessage;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.DatatypeMessage;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;

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
public abstract class AbstractFixedPointDataSource<T> {
    protected final FileChannel fileChannel;
    protected final long startOffset;
    protected final int recordSize;
    protected final int readsAvailable;
    protected final int elementsPerRecord;
    protected final FixedPointDatatype fixedPointDatatype;
    protected final int scale;
    protected final long sizeForReadBuffer;
    protected final long endOffset;

    /**
     * Constructs the base data source with HDF5 metadata and optional FileChannel for streaming.
     *
     * @param headerPrefixV1 the HDF5 object header prefix containing datatype and dataspace metadata
     * @param scale the scale for BigDecimal values (number of decimal places); use 0 for BigInteger
     * @param fileChannel the FileChannel to read the HDF5 file data from, or null if not streaming
     * @param startOffset the byte offset in the file where the dataset begins
     * @throws IllegalStateException if required metadata (DatatypeMessage or DataspaceMessage) is missing
     * @throws IllegalArgumentException if dataset dimensionality is unsupported
     */
    public AbstractFixedPointDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, int scale, FileChannel fileChannel, long startOffset) {
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

        this.readsAvailable = dimensions[0].toBigInteger().intValue();
        this.fixedPointDatatype = (FixedPointDatatype) headerPrefixV1.findMessageByType(DatatypeMessage.class)
                .orElseThrow()
                .getHdfDatatype();

        if (dimensions.length == 1) {
            this.elementsPerRecord = 1;
        } else if (dimensions.length == 2) {
            this.elementsPerRecord = dimensions[1].toBigInteger().intValue();
        } else {
            throw new IllegalArgumentException("Unsupported dimensionality: " + dimensions.length);
        }

        this.sizeForReadBuffer = (long) recordSize * elementsPerRecord;
        this.endOffset = fileChannel != null ? startOffset + sizeForReadBuffer * readsAvailable : 0;
    }

    /**
     * Returns the size in bytes of one record in the HDF5 dataset.
     * For vectors (1D), this is the size of a single fixed-point value. For matrices (2D),
     * this is the size of one row (element size times number of elements per row).
     *
     * @return the size in bytes of one record
     */
    public long getSizeForReadBuffer() {
        return sizeForReadBuffer;
    }

    /**
     * Returns the number of records available to read from the HDF5 dataset.
     * This corresponds to the first dimension of the dataset (e.g., number of scalars for vectors,
     * number of rows for matrices), as defined in the HDF5 metadata.
     *
     * @return the number of records available
     */
    public long getNumberOfReadsAvailable() {
        return readsAvailable;
    }

    /**
     * Populates an object of type T from the provided ByteBuffer.
     * Subclasses must implement this to define how data is mapped from the buffer to T.
     *
     * @param buffer the ByteBuffer containing the HDF5 dataset data
     * @return an object of type T populated from the buffer
     */
    protected abstract T populateFromBufferRaw(ByteBuffer buffer);

    /**
     * Returns a sequential Stream for reading data from the associated FileChannel.
     *
     * @return a sequential Stream of T
     * @throws IllegalStateException if no FileChannel was provided in the constructor
     */
    public abstract Stream<T> stream();

    /**
     * Returns a parallel Stream for reading data from the associated FileChannel.
     *
     * @return a parallel Stream of T
     * @throws IllegalStateException if no FileChannel was provided in the constructor
     */
    public abstract Stream<T> parallelStream();

    /**
     * Base Spliterator for streaming data from the FileChannel.
     */
    protected class FixedPointSpliterator implements Spliterator<T> {
        private final long endOffset;
        private long currentOffset;

        public FixedPointSpliterator(long startOffset, long endOffset) {
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

            Spliterator<T> newSpliterator = new FixedPointSpliterator(currentOffset, splitOffset);
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