package org.hdf5javalib.datasource;

import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.utils.FlattenedArrayUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TypedDataSource<T> {
    private final HdfDataSet dataset;
    private final FileChannel fileChannel;
//    private final long startOffset;
    private final Class<T> dataClass;
    private final int[] dimensions;
    private final int elementSize;
    private final HdfGlobalHeap globalHeap;

    public TypedDataSource(HdfDataSet dataset, FileChannel fileChannel, Class<T> dataClass) {
        this.dataset = dataset;
        this.fileChannel = fileChannel;
        this.globalHeap = new HdfGlobalHeap(this::initializeGlobalHeap);
//        this.startOffset = startOffset;
        this.dataClass = dataClass;
        this.elementSize = dataset.getHdfDatatype().getSize();
        this.dimensions = extractDimensions(dataset.getDataObjectHeaderPrefix()
                .findMessageByType(DataspaceMessage.class)
                .orElseThrow(() -> new IllegalStateException("DataspaceMessage not found")));
        dataset.getDataObjectHeaderPrefix().findMessageByType(DatatypeMessage.class).orElseThrow().getHdfDatatype().setGlobalHeap(globalHeap);

    }

    public int[] getShape() {
        return dimensions.clone();
    }

    private void initializeGlobalHeap(int length, long offset, int index) {
        try {
            fileChannel.position(offset);
            globalHeap.readFromFileChannel(fileChannel, (short)8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int[] extractDimensions(DataspaceMessage dataspace) {
        HdfFixedPoint[] dims = dataspace.getDimensions();
        int[] result = new int[dims.length];
        for (int i = 0; i < dims.length; i++) {
            result[i] = dims[i].getInstance(Long.class).intValue();
        }
        return result;
    }

    private ByteBuffer readBytes(long offset, long size) throws IOException {
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Size too large: " + size);
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
        synchronized (fileChannel) {
            fileChannel.position(dataset.getDataAddress().getInstance(Long.class) + offset);
            int bytesRead = fileChannel.read(buffer);
            if (bytesRead != size) {
                throw new IOException("Failed to read the expected number of bytes: read " + bytesRead + ", expected " + size);
            }
            buffer.flip();
            return buffer;
        }
    }

    private T populateElement(ByteBuffer buffer) {
        byte[] bytes = new byte[elementSize];
        buffer.get(bytes);
        return dataset.getHdfDatatype().getInstance(dataClass, bytes);
    }

    private T[] populateVector(ByteBuffer buffer, int length) {
        @SuppressWarnings("unchecked")
        T[] vector = (T[]) Array.newInstance(dataClass, length);
        for (int i = 0; i < length; i++) {
            vector[i] = populateElement(buffer);
        }
        return vector;
    }

    private T[][] populateMatrix(ByteBuffer buffer, int rows, int cols) {
        @SuppressWarnings("unchecked")
        T[][] matrix = (T[][]) Array.newInstance(dataClass, rows, cols);
        for (int i = 0; i < rows; i++) {
            matrix[i] = populateVector(buffer, cols);
        }
        return matrix;
    }

    private T[][][] populateTensor(ByteBuffer buffer, int depth, int rows, int cols) {
        @SuppressWarnings("unchecked")
        T[][][] tensor = (T[][][]) Array.newInstance(dataClass, depth, rows, cols);
        for (int d = 0; d < depth; d++) {
            tensor[d] = populateMatrix(buffer, rows, cols);
        }
        return tensor;
    }

    // --- Scalar (0D) Methods ---

    public T readScalar() throws IOException {
        if (dimensions.length != 0) {
            throw new IllegalStateException("Dataset must be 0D");
        }
        ByteBuffer buffer = readBytes(0, elementSize);
        return populateElement(buffer);
    }

    public Stream<T> streamScalar() {
        if (dimensions.length != 0) {
            throw new IllegalStateException("Dataset must be 0D");
        }
        try {
            return Stream.of(readScalar());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Stream<T> parallelStreamScalar() {
        return streamScalar(); // Parallelism not applicable for single element
    }

    // --- Vector (1D) Methods ---

    public T[] readVector() throws IOException {
        if (dimensions.length != 1) {
            throw new IllegalStateException("Dataset must be 1D");
        }
        int size = dimensions[0];
        ByteBuffer buffer = readBytes(0, (long) elementSize * size);
        return populateVector(buffer, size);
    }

    public Stream<T> streamVector() {
        if (dimensions.length != 1) {
            throw new IllegalStateException("Dataset must be 1D");
        }
        return StreamSupport.stream(new VectorSpliterator(0, dimensions[0], elementSize), false);
    }

    public Stream<T> parallelStreamVector() {
        if (dimensions.length != 1) {
            throw new IllegalStateException("Dataset must be 1D");
        }
        return StreamSupport.stream(new VectorSpliterator(0, dimensions[0], elementSize), true);
    }

    // --- Matrix (2D) Methods ---

    public T[][] readMatrix() throws IOException {
        if (dimensions.length != 2) {
            throw new IllegalStateException("Dataset must be 2D");
        }
        int rows = dimensions[0];
        int cols = dimensions[1];
        ByteBuffer buffer = readBytes(0, (long) elementSize * rows * cols);
        return populateMatrix(buffer, rows, cols);
    }

    public Stream<T[]> streamMatrix() {
        if (dimensions.length != 2) {
            throw new IllegalStateException("Dataset must be 2D");
        }
        long rowSize = (long) elementSize * dimensions[1];
        return StreamSupport.stream(new MatrixSpliterator(0, dimensions[0], rowSize, dimensions[1]), false);
    }

    public Stream<T[]> parallelStreamMatrix() {
        if (dimensions.length != 2) {
            throw new IllegalStateException("Dataset must be 2D");
        }
        long rowSize = (long) elementSize * dimensions[1];
        return StreamSupport.stream(new MatrixSpliterator(0, dimensions[0], rowSize, dimensions[1]), true);
    }

    // --- Tensor (3D) Methods ---

    public T[][][] readTensor() throws IOException {
        if (dimensions.length != 3) {
            throw new IllegalStateException("Dataset must be 3D");
        }
        int depth = dimensions[0];
        int rows = dimensions[1];
        int cols = dimensions[2];
        ByteBuffer buffer = readBytes(0, (long) elementSize * depth * rows * cols);
        return populateTensor(buffer, depth, rows, cols);
    }

    public Stream<T[][]> streamTensor() {
        if (dimensions.length != 3) {
            throw new IllegalStateException("Dataset must be 3D");
        }
        long sliceSize = (long) elementSize * dimensions[1] * dimensions[2];
        return StreamSupport.stream(new TensorSpliterator(0, dimensions[0], sliceSize, dimensions[1], dimensions[2]), false);
    }

    public Stream<T[][]> parallelStreamTensor() {
        if (dimensions.length != 3) {
            throw new IllegalStateException("Dataset must be 3D");
        }
        long sliceSize = (long) elementSize * dimensions[1] * dimensions[2];
        return StreamSupport.stream(new TensorSpliterator(0, dimensions[0], sliceSize, dimensions[1], dimensions[2]), true);
    }

    // --- Flattened Methods ---

    public T[] readFlattened() throws IOException {
        int totalElements = FlattenedArrayUtils.totalSize(dimensions);
        ByteBuffer buffer = readBytes(0, (long) elementSize * totalElements);
        return populateVector(buffer, totalElements);
    }

    public Stream<T> streamFlattened() {
        int totalElements = FlattenedArrayUtils.totalSize(dimensions);
        return StreamSupport.stream(new FlattenedSpliterator(0, totalElements, elementSize), false);
    }

    public Stream<T> parallelStreamFlattened() {
        int totalElements = FlattenedArrayUtils.totalSize(dimensions);
        return StreamSupport.stream(new FlattenedSpliterator(0, totalElements, elementSize), true);
    }

    // --- Spliterators ---

    private abstract class AbstractSpliterator<R> implements Spliterator<R> {
        private long currentIndex;
        private final long limit;
        private final long recordSize;

        public AbstractSpliterator(long start, long limit, long recordSize) {
            this.currentIndex = start;
            this.limit = limit;
            this.recordSize = recordSize;
        }

        @Override
        public boolean tryAdvance(Consumer<? super R> action) {
            if (currentIndex >= limit) {
                return false;
            }
            try {
                long offset = currentIndex * recordSize;
                ByteBuffer buffer = readBytes(offset, recordSize);
                R record = populateRecord(buffer);
                action.accept(record);
                currentIndex++;
                return true;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public Spliterator<R> trySplit() {
            long remaining = limit - currentIndex;
            if (remaining <= 1) {
                return null;
            }
            long splitIndex = currentIndex + remaining / 2;
            Spliterator<R> newSpliterator = createNewSpliterator(currentIndex, splitIndex, recordSize);
            currentIndex = splitIndex;
            return newSpliterator;
        }

        @Override
        public long estimateSize() {
            return limit - currentIndex;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | SIZED | SUBSIZED;
        }

        protected abstract R populateRecord(ByteBuffer buffer);
        protected abstract Spliterator<R> createNewSpliterator(long start, long end, long recordSize);
    }

    private class VectorSpliterator extends AbstractSpliterator<T> {
        public VectorSpliterator(long start, long limit, long recordSize) {
            super(start, limit, recordSize);
        }

        @Override
        protected T populateRecord(ByteBuffer buffer) {
            return populateElement(buffer);
        }

        @Override
        protected Spliterator<T> createNewSpliterator(long start, long end, long recordSize) {
            return new VectorSpliterator(start, end, recordSize);
        }
    }

    private class MatrixSpliterator extends AbstractSpliterator<T[]> {
        private final int cols;

        public MatrixSpliterator(long start, long limit, long recordSize, int cols) {
            super(start, limit, recordSize);
            this.cols = cols;
        }

        @Override
        protected T[] populateRecord(ByteBuffer buffer) {
            return populateVector(buffer, cols);
        }

        @Override
        protected Spliterator<T[]> createNewSpliterator(long start, long end, long recordSize) {
            return new MatrixSpliterator(start, end, recordSize, cols);
        }
    }

    private class TensorSpliterator extends AbstractSpliterator<T[][]> {
        private final int rows;
        private final int cols;

        public TensorSpliterator(long start, long limit, long recordSize, int rows, int cols) {
            super(start, limit, recordSize);
            this.rows = rows;
            this.cols = cols;
        }

        @Override
        protected T[][] populateRecord(ByteBuffer buffer) {
            return populateMatrix(buffer, rows, cols);
        }

        @Override
        protected Spliterator<T[][]> createNewSpliterator(long start, long end, long recordSize) {
            return new TensorSpliterator(start, end, recordSize, rows, cols);
        }
    }

    private class FlattenedSpliterator extends AbstractSpliterator<T> {
        public FlattenedSpliterator(long start, long limit, long recordSize) {
            super(start, limit, recordSize);
        }

        @Override
        protected T populateRecord(ByteBuffer buffer) {
            return populateElement(buffer);
        }

        @Override
        protected Spliterator<T> createNewSpliterator(long start, long end, long recordSize) {
            return new FlattenedSpliterator(start, end, recordSize);
        }
    }
}