package org.hdf5javalib.datasource;

import org.hdf5javalib.hdffile.dataobjects.messages.DatatypeMessage;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.utils.FlattenedArrayUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A data source for reading typed data from an HDF5 dataset.
 * <p>
 * The {@code TypedDataSource} class provides methods to read and stream data from an
 * HDF5 dataset, supporting scalar (0D), vector (1D), matrix (2D), and tensor (3D)
 * data structures. It uses a {@link SeekableByteChannel} to access the file and
 * interprets the data according to the dataset's datatype and dimensions.
 * </p>
 *
 * @param <T> the Java type of the data elements (e.g., {@link Integer}, {@link Double})
 * @see HdfDataset
 * @see HdfDataFile
 */
public class TypedDataSource<T> {
    /**
     * The HDF5 dataset being accessed.
     */
    private final HdfDataset dataset;
    /**
     * The channel for reading data from the HDF5 file.
     */
    private final SeekableByteChannel channel;
    /**
     * The Java class of the data elements.
     */
    private final Class<T> dataClass;
    /**
     * The dimensions of the dataset.
     */
    private final int[] dimensions;
    /**
     * The size of each data element in bytes.
     */
    private final int elementSize;

    /**
     * Constructs a TypedDataSource for the specified dataset and data type.
     *
     * @param channel     the SeekableByteChannel for reading the HDF5 file
     * @param hdfDataFile the HDF5 file context for global heap and other resources
     * @param dataset     the HDF5 dataset to read from
     * @param dataClass   the Java class of the data elements
     * @throws NullPointerException if any parameter is null
     */
    public TypedDataSource(SeekableByteChannel channel, HdfDataFile hdfDataFile, HdfDataset dataset, Class<T> dataClass) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (channel == null || hdfDataFile == null || dataset == null || dataClass == null) {
            throw new NullPointerException("Parameters must not be null");
        }
        this.dataset = dataset;
        this.channel = channel;
        this.dataClass = dataClass;
        this.elementSize = dataset.getElementSize();
        this.dimensions = dataset.extractDimensions();
        dataset.getObjectHeader().findMessageByType(DatatypeMessage.class).orElseThrow()
                .getHdfDatatype().setGlobalHeap(hdfDataFile.getGlobalHeap());
    }

    /**
     * Returns a copy of the dataset's shape (dimensions).
     *
     * @return a cloned array of the dataset dimensions
     */
    public int[] getShape() {
        return dimensions.clone();
    }

    /**
     * Reads a specified number of bytes from the dataset at the given offset.
     *
     * @param offset the starting offset in the dataset
     * @param size   the number of bytes to read
     * @return a ByteBuffer containing the read data
     * @throws IOException              if an I/O error occurs
     * @throws IllegalArgumentException if the size exceeds Integer.MAX_VALUE
     */
    private ByteBuffer readBytes(long offset, long size) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Size too large: " + size);
        }
        return dataset.getDatasetData(channel, offset, size);
    }

    /**
     * Populates a single element from the ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the element data
     * @return the element converted to the specified Java type
     */
    private T populateElement(ByteBuffer buffer) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        byte[] bytes = new byte[elementSize];
        buffer.get(bytes);
        return dataset.getDatatype().getInstance(dataClass, bytes);
    }

    /**
     * Populates a vector (1D array) from the ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the vector data
     * @param length the length of the vector
     * @return the populated vector
     */
    private T[] populateVector(ByteBuffer buffer, int length) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        @SuppressWarnings("unchecked")
        T[] vector = (T[]) Array.newInstance(dataClass, length);
        for (int i = 0; i < length; i++) {
            vector[i] = populateElement(buffer);
        }
        return vector;
    }

    /**
     * Populates a matrix (2D array) from the ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the matrix data
     * @param rows   the number of rows
     * @param cols   the number of columns
     * @return the populated matrix
     */
    private T[][] populateMatrix(ByteBuffer buffer, int rows, int cols) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        @SuppressWarnings("unchecked")
        T[][] matrix = (T[][]) Array.newInstance(dataClass, rows, cols);
        for (int i = 0; i < rows; i++) {
            matrix[i] = populateVector(buffer, cols);
        }
        return matrix;
    }

    /**
     * Populates a tensor (3D array) from the ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the tensor data
     * @param depth  the depth of the tensor
     * @param rows   the number of rows per slice
     * @param cols   the number of columns per slice
     * @return the populated tensor
     */
    private T[][][] populateTensor(ByteBuffer buffer, int depth, int rows, int cols) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        @SuppressWarnings("unchecked")
        T[][][] tensor = (T[][][]) Array.newInstance(dataClass, depth, rows, cols);
        for (int d = 0; d < depth; d++) {
            tensor[d] = populateMatrix(buffer, rows, cols);
        }
        return tensor;
    }

    // --- Scalar (0D) Methods ---

    /**
     * Reads a scalar (0D) value from the dataset.
     *
     * @return the scalar value
     * @throws IOException           if an I/O error occurs
     * @throws IllegalStateException if the dataset is not 0D
     */
    public T readScalar() throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (dimensions.length != 0) {
            throw new IllegalStateException("Dataset must be 0D(Scalar)");
        }
        if (!dataset.hasData()) {
            throw new IllegalStateException("Dataset has no data");
        }
        ByteBuffer buffer = readBytes(0, elementSize);
        return populateElement(buffer);
    }

    /**
     * Streams a scalar (0D) value from the dataset.
     *
     * @return a Stream containing the scalar value
     * @throws UncheckedIOException  if an I/O error occurs
     * @throws IllegalStateException if the dataset is not 0D
     */
    public Stream<T> streamScalar() throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (dimensions.length != 0) {
            throw new IllegalStateException("Dataset must be 0D(Scalar)");
        }
        if (!dataset.hasData()) {
            throw new IllegalStateException("Dataset has no data");
        }
        return Stream.of(readScalar());
    }

    /**
     * Streams a scalar (0D) value from the dataset (non-parallel).
     *
     * @return a Stream containing the scalar value
     * @throws UncheckedIOException  if an I/O error occurs
     * @throws IllegalStateException if the dataset is not 0D
     */
    public Stream<T> parallelStreamScalar() throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        return streamScalar(); // Parallelism not applicable for single element
    }

    // --- Vector (1D) Methods ---

    /**
     * Reads a vector (1D) from the dataset.
     *
     * @return the vector as an array
     * @throws IOException           if an I/O error occurs
     * @throws IllegalStateException if the dataset is not 1D
     */
    public T[] readVector() throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (dimensions.length != 1) {
            throw new IllegalStateException("Dataset must be 1D(Vector)");
        }
        int size = dimensions[0];
        ByteBuffer buffer = readBytes(0, (long) elementSize * size);
        return populateVector(buffer, size);
    }

    /**
     * Streams a vector (1D) from the dataset.
     *
     * @return a Stream of vector elements
     * @throws IllegalStateException if the dataset is not 1D
     */
    public Stream<T> streamVector() {
        if (dimensions.length != 1) {
            throw new IllegalStateException("Dataset must be 1D(Vector)");
        }
        return StreamSupport.stream(new VectorSpliterator(0, dimensions[0], elementSize), false);
    }

    /**
     * Streams a vector (1D) from the dataset in parallel.
     *
     * @return a parallel Stream of vector elements
     * @throws IllegalStateException if the dataset is not 1D
     */
    public Stream<T> parallelStreamVector() {
        if (dimensions.length != 1) {
            throw new IllegalStateException("Dataset must be 1D(Vector)");
        }
        return StreamSupport.stream(new VectorSpliterator(0, dimensions[0], elementSize), true);
    }

    // --- Matrix (2D) Methods ---

    /**
     * Reads a matrix (2D) from the dataset.
     *
     * @return the matrix as a 2D array
     * @throws IOException           if an I/O error occurs
     * @throws IllegalStateException if the dataset is not 2D
     */
    public T[][] readMatrix() throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (dimensions.length != 2) {
            throw new IllegalStateException("Dataset must be 2D(Matrix)");
        }
        int rows = dimensions[0];
        int cols = dimensions[1];
        ByteBuffer buffer = readBytes(0, (long) elementSize * rows * cols);
        return populateMatrix(buffer, rows, cols);
    }

    /**
     * Streams a matrix (2D) from the dataset as rows.
     *
     * @return a Stream of matrix rows
     * @throws IllegalStateException if the dataset is not 2D
     */
    public Stream<T[]> streamMatrix() {
        if (dimensions.length != 2) {
            throw new IllegalStateException("Dataset must be 2D(Matrix)");
        }
        long rowSize = (long) elementSize * dimensions[1];
        return StreamSupport.stream(new MatrixSpliterator(0, dimensions[0], rowSize, dimensions[1]), false);
    }

    /**
     * Streams a matrix (2D) from the dataset as rows in parallel.
     *
     * @return a parallel Stream of matrix rows
     * @throws IllegalStateException if the dataset is not 2D
     */
    public Stream<T[]> parallelStreamMatrix() {
        if (dimensions.length != 2) {
            throw new IllegalStateException("Dataset must be 2D(Matrix)");
        }
        long rowSize = (long) elementSize * dimensions[1];
        return StreamSupport.stream(new MatrixSpliterator(0, dimensions[0], rowSize, dimensions[1]), true);
    }

    // --- Tensor (3D) Methods ---

    /**
     * Reads a tensor (3D) from the dataset.
     *
     * @return the tensor as a 3D array
     * @throws IOException           if an I/O error occurs
     * @throws IllegalStateException if the dataset is not 3D
     */
    public T[][][] readTensor() throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (dimensions.length != 3) {
            throw new IllegalStateException("Dataset must be 3D(Tensor)");
        }
        int depth = dimensions[0];
        int rows = dimensions[1];
        int cols = dimensions[2];
        ByteBuffer buffer = readBytes(0, (long) elementSize * depth * rows * cols);
        return populateTensor(buffer, depth, rows, cols);
    }

    /**
     * Streams a tensor (3D) from the dataset as 2D slices.
     *
     * @return a Stream of matrix slices
     * @throws IllegalStateException if the dataset is not 3D
     */
    public Stream<T[][]> streamTensor() {
        if (dimensions.length != 3) {
            throw new IllegalStateException("Dataset must be 3D(Tensor)");
        }
        long sliceSize = (long) elementSize * dimensions[1] * dimensions[2];
        return StreamSupport.stream(new TensorSpliterator(0, dimensions[0], sliceSize, dimensions[1], dimensions[2]), false);
    }

    /**
     * Streams a tensor (3D) from the dataset as 2D slices in parallel.
     *
     * @return a parallel Stream of matrix slices
     * @throws IllegalStateException if the dataset is not 3D
     */
    public Stream<T[][]> parallelStreamTensor() {
        if (dimensions.length != 3) {
            throw new IllegalStateException("Dataset must be 3D(Tensor)");
        }
        long sliceSize = (long) elementSize * dimensions[1] * dimensions[2];
        return StreamSupport.stream(new TensorSpliterator(0, dimensions[0], sliceSize, dimensions[1], dimensions[2]), true);
    }

    // --- Flattened Methods ---

    /**
     * Reads the dataset as a flattened (1D) array.
     *
     * @return the flattened array
     * @throws IOException if an I/O error occurs
     */
    public T[] readFlattened() throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int totalElements = FlattenedArrayUtils.totalSize(dimensions);
        ByteBuffer buffer = readBytes(0, (long) elementSize * totalElements);
        return populateVector(buffer, totalElements);
    }

    /**
     * Streams the dataset as a flattened (1D) sequence.
     *
     * @return a Stream of all elements
     */
    public Stream<T> streamFlattened() {
        int totalElements = FlattenedArrayUtils.totalSize(dimensions);
        return StreamSupport.stream(new FlattenedSpliterator(0, totalElements, elementSize), false);
    }

    /**
     * Streams the dataset as a flattened (1D) sequence in parallel.
     *
     * @return a parallel Stream of all elements
     */
    public Stream<T> parallelStreamFlattened() {
        int totalElements = FlattenedArrayUtils.totalSize(dimensions);
        return StreamSupport.stream(new FlattenedSpliterator(0, totalElements, elementSize), true);
    }

    // --- Spliterators ---

    /**
     * Abstract base class for dataset spliterators.
     *
     * @param <R> the type of elements produced by the spliterator
     */
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
            long offset = currentIndex * recordSize;
            ByteBuffer buffer = null;
            try {
                buffer = readBytes(offset, recordSize);
                R record = populateRecord(buffer);
                action.accept(record);
                currentIndex++;
                return true;
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException | IOException e) {
                throw new IllegalStateException(e);
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

        /**
         * Populates a record from the ByteBuffer.
         *
         * @param buffer the ByteBuffer containing the record data
         * @return the populated record
         */
        protected abstract R populateRecord(ByteBuffer buffer) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException;

        /**
         * Creates a new spliterator for a split range.
         *
         * @param start      the start index
         * @param end        the end index
         * @param recordSize the size of each record
         * @return a new Spliterator
         */
        protected abstract Spliterator<R> createNewSpliterator(long start, long end, long recordSize);
    }

    /**
     * Spliterator for streaming vector (1D) elements.
     */
    private class VectorSpliterator extends AbstractSpliterator<T> {
        public VectorSpliterator(long start, long limit, long recordSize) {
            super(start, limit, recordSize);
        }

        @Override
        protected T populateRecord(ByteBuffer buffer) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
            return populateElement(buffer);
        }

        @Override
        protected Spliterator<T> createNewSpliterator(long start, long end, long recordSize) {
            return new VectorSpliterator(start, end, recordSize);
        }
    }

    /**
     * Spliterator for streaming matrix (2D) rows.
     */
    private class MatrixSpliterator extends AbstractSpliterator<T[]> {
        private final int cols;

        public MatrixSpliterator(long start, long limit, long recordSize, int cols) {
            super(start, limit, recordSize);
            this.cols = cols;
        }

        @Override
        protected T[] populateRecord(ByteBuffer buffer) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
            return populateVector(buffer, cols);
        }

        @Override
        protected Spliterator<T[]> createNewSpliterator(long start, long end, long recordSize) {
            return new MatrixSpliterator(start, end, recordSize, cols);
        }
    }

    /**
     * Spliterator for streaming tensor (3D) slices.
     */
    private class TensorSpliterator extends AbstractSpliterator<T[][]> {
        private final int rows;
        private final int cols;

        public TensorSpliterator(long start, long limit, long recordSize, int rows, int cols) {
            super(start, limit, recordSize);
            this.rows = rows;
            this.cols = cols;
        }

        @Override
        protected T[][] populateRecord(ByteBuffer buffer) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
            return populateMatrix(buffer, rows, cols);
        }

        @Override
        protected Spliterator<T[][]> createNewSpliterator(long start, long end, long recordSize) {
            return new TensorSpliterator(start, end, recordSize, rows, cols);
        }
    }

    /**
     * Spliterator for streaming flattened dataset elements.
     */
    private class FlattenedSpliterator extends AbstractSpliterator<T> {
        public FlattenedSpliterator(long start, long limit, long recordSize) {
            super(start, limit, recordSize);
        }

        @Override
        protected T populateRecord(ByteBuffer buffer) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
            return populateElement(buffer);
        }

        @Override
        protected Spliterator<T> createNewSpliterator(long start, long end, long recordSize) {
            return new FlattenedSpliterator(start, end, recordSize);
        }
    }
}