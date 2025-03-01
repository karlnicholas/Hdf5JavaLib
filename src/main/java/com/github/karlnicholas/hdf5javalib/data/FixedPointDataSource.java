package com.github.karlnicholas.hdf5javalib.data;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.message.DataspaceMessage;
import com.github.karlnicholas.hdf5javalib.message.DatatypeMessage;
import com.github.karlnicholas.hdf5javalib.message.datatype.FixedPointDatatype;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A data source for reading and writing fixed-point data (BigInteger or BigDecimal) from HDF5 files.
 * This class supports both scalar (vector) and array (matrix) data types, providing methods to populate
 * objects from ByteBuffers and write objects back to ByteBuffers. When initialized with a FileChannel,
 * it also enables streaming data directly from an HDF5 file using sequential or parallel streams.
 *
 * <p>The class is designed to work with HDF5 datasets containing fixed-point data, using metadata
 * from an {@link HdfObjectHeaderPrefixV1} to determine dimensionality and data type. It supports
 * two constructors: one for manual buffer operations and another for streaming from a file.</p>
 *
 * <p><strong>Usage Examples:</strong></p>
 *
 * <pre>
 * // Example 1: Manual buffer operations (no streaming)
 * FixedPointDataSource&lt;Scalar&gt; dataSource = new FixedPointDataSource&lt;&gt;(
 *     headerPrefix, "data", 0, Scalar.class
 * );
 * ByteBuffer buffer = ByteBuffer.allocate((int) dataSource.getSizeForReadBuffer());
 * // Assume buffer is filled with data from an HDF5 file
 * Scalar scalar = dataSource.populateFromBuffer(buffer);
 * System.out.println(scalar.value); // Assuming Scalar has a BigInteger 'value' field
 *
 * // Example 2: Streaming from a FileChannel
 * FixedPointDataSource&lt;Matrix&gt; dataSource = new FixedPointDataSource&lt;&gt;(
 *     headerPrefix, "values", 2, Matrix.class, fileChannel, dataAddress
 * );
 * // Sequential stream
 * dataSource.stream().forEach(m -> System.out.println(Arrays.toString(m.values)));
 * // Parallel stream
 * dataSource.parallelStream().mapToInt(m -> m.values.length).sum();
 * </pre>
 *
 * <p><strong>Notes:</strong></p>
 * <ul>
 *   <li>For vector data (1D), the target field in T must be BigInteger or BigDecimal.</li>
 *   <li>For matrix data (2D), the target field must be BigInteger[] or BigDecimal[].</li>
 *   <li>Streaming requires the FileChannel constructor; otherwise, use buffer methods manually.</li>
 * </ul>
 *
 * @param <T> the type of object to populate or write, containing a field matching the HDF5 dataset
 */
public class FixedPointDataSource<T> {
    private final Class<T> clazz;
    private final Field field;
    private final int recordSize;
    private final int readsAvailable;
    private final int elementsPerRecord;
    private final FixedPointDatatype fixedPointDatatype;
    private final int scale;
    private final FileChannel fileChannel; // Nullable
    private final long startOffset; // Only used if fileChannel is present
    private final long sizeForReadBuffer;
    private final long endOffset; // Only used if fileChannel is present

    /**
     * Constructs a FixedPointDataSource for manual buffer operations without streaming capabilities.
     * Use this constructor when you need to read from or write to ByteBuffers directly, without
     * accessing an HDF5 file via a FileChannel. The object will be configured based on the provided
     * HDF5 metadata but will not support stream() or parallelStream() methods.
     *
     * @param headerPrefixV1 the HDF5 object header prefix containing datatype and dataspace metadata
     * @param name the name of the field in the target class T to map to the HDF5 dataset
     * @param scale the scale for BigDecimal values (number of decimal places); use 0 for BigInteger
     * @param clazz the class of the target object T to populate or write
     * @throws IllegalStateException if required metadata (DatatypeMessage or DataspaceMessage) is missing
     * @throws IllegalArgumentException if the field name is not found in T or field type is incompatible
     */
    public FixedPointDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz) {
        this(headerPrefixV1, name, scale, clazz, null, 0);
    }

    /**
     * Constructs a FixedPointDataSource for both manual buffer operations and streaming from an HDF5 file.
     * Use this constructor when you want to stream data directly from a FileChannel, in addition to
     * supporting buffer-based read/write operations. The object will be configured based on the HDF5
     * metadata and will enable stream() and parallelStream() methods starting at the specified data address.
     *
     * @param headerPrefixV1 the HDF5 object header prefix containing datatype and dataspace metadata
     * @param name the name of the field in the target class T to map to the HDF5 dataset
     * @param scale the scale for BigDecimal values (number of decimal places); use 0 for BigInteger
     * @param clazz the class of the target object T to populate or write
     * @param fileChannel the FileChannel to read the HDF5 file data from
     * @param startOffset the byte offset in the file where the dataset begins
     * @throws IllegalStateException if required metadata (DatatypeMessage or DataspaceMessage) is missing
     * @throws IllegalArgumentException if the field name is not found in T or field type is incompatible
     */
    public FixedPointDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz, FileChannel fileChannel, long startOffset) {
        this.clazz = clazz;
        this.scale = scale;
        this.fileChannel = fileChannel;
        this.startOffset = startOffset;

        recordSize = headerPrefixV1.findMessageByType(DatatypeMessage.class)
                .orElseThrow(() -> new IllegalStateException("DatatypeMessage not found"))
                .getHdfDatatype()
                .getSize();

        HdfFixedPoint[] dimensions = headerPrefixV1.findMessageByType(DataspaceMessage.class)
                .orElseThrow(() -> new IllegalStateException("DataspaceMessage not found"))
                .getDimensions();

        readsAvailable = dimensions[0].toBigInteger().intValue();
        fixedPointDatatype = (FixedPointDatatype) headerPrefixV1.findMessageByType(DatatypeMessage.class)
                .orElseThrow()
                .getHdfDatatype();

        if (dimensions.length == 1) {
            elementsPerRecord = 1;
        } else if (dimensions.length == 2) {
            elementsPerRecord = dimensions[1].toBigInteger().intValue();
        } else {
            throw new IllegalArgumentException("Unsupported dimensionality: " + dimensions.length);
        }

        Field fieldToSet = null;
        for (Field f : clazz.getDeclaredFields()) {
            f.setAccessible(true);
            if (f.getName().equals(name)) {
                fieldToSet = f;
                break;
            }
        }
        if (fieldToSet == null) {
            throw new IllegalArgumentException("Field " + name + " not found in " + clazz.getName());
        }
        this.field = fieldToSet;

        Class<?> fieldType = field.getType();
        if (elementsPerRecord == 1) {
            if (!(fieldType.equals(BigDecimal.class) || fieldType.equals(BigInteger.class))) {
                throw new IllegalArgumentException("Vector field must be BigDecimal or BigInteger, got " + fieldType);
            }
        } else {
            if (!(fieldType.isArray() &&
                    (fieldType.getComponentType().equals(BigDecimal.class) ||
                            fieldType.getComponentType().equals(BigInteger.class)))) {
                throw new IllegalArgumentException("Matrix field must be BigDecimal[] or BigInteger[], got " + fieldType);
            }
        }

        this.sizeForReadBuffer = (long) recordSize * elementsPerRecord;
        this.endOffset = fileChannel != null ? startOffset + sizeForReadBuffer * readsAvailable : 0;
    }

    /**
     * Populates a new instance of T with fixed-point data from the provided ByteBuffer.
     * Depending on the dataset's dimensionality and scale, the target field in T will be set to
     * a BigInteger (scale = 0) or BigDecimal (scale > 0) for vectors, or an array of the same
     * for matrices.
     *
     * @param buffer the ByteBuffer containing the HDF5 dataset data, positioned at the start of a record
     * @return a new instance of T with the field populated from the buffer
     * @throws RuntimeException if instantiation or field setting fails (e.g., due to reflection errors)
     */
    public T populateFromBuffer(ByteBuffer buffer) {
        try {
            T instance = clazz.getDeclaredConstructor().newInstance();
            if (elementsPerRecord == 1) {
                if (scale > 0) {
                    BigDecimal value = fixedPointDatatype.getInstance(buffer).toBigDecimal(scale);
                    field.set(instance, value);
                } else {
                    BigInteger value = fixedPointDatatype.getInstance(buffer).toBigInteger();
                    field.set(instance, value);
                }
            } else {
                if (scale > 0) {
                    BigDecimal[] data = new BigDecimal[elementsPerRecord];
                    for (int i = 0; i < elementsPerRecord; i++) {
                        data[i] = fixedPointDatatype.getInstance(buffer).toBigDecimal(scale);
                    }
                    field.set(instance, data);
                } else {
                    BigInteger[] data = new BigInteger[elementsPerRecord];
                    for (int i = 0; i < elementsPerRecord; i++) {
                        data[i] = fixedPointDatatype.getInstance(buffer).toBigInteger();
                    }
                    field.set(instance, data);
                }
            }
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error creating and populating instance of " + clazz.getName(), e);
        }
    }

    /**
     * Writes the data from an instance of T into the provided ByteBuffer.
     * The target field’s value (BigInteger or BigDecimal for vectors, or their arrays for matrices)
     * is converted to bytes according to the HDF5 fixed-point datatype and written to the buffer.
     *
     * @param instance the object T whose field data will be written
     * @param buffer the ByteBuffer to write the data into
     * @throws RuntimeException if field access or writing fails (e.g., due to reflection or buffer errors)
     * @throws IllegalArgumentException if a matrix field array size does not match the expected dimensions
     */
    public void writeToBuffer(T instance, ByteBuffer buffer) {
        try {
            if (elementsPerRecord == 1) {
                if (scale > 0) {
                    BigDecimal value = (BigDecimal) field.get(instance);
                    new HdfFixedPoint(
                            value.unscaledValue().toByteArray(),
                            fixedPointDatatype.getSize(),
                            fixedPointDatatype.isBigEndian(),
                            fixedPointDatatype.isLopad(),
                            fixedPointDatatype.isHipad(),
                            fixedPointDatatype.isSigned(),
                            fixedPointDatatype.getBitOffset(),
                            fixedPointDatatype.getBitPrecision()
                    ).writeValueToByteBuffer(buffer);
                } else {
                    BigInteger value = (BigInteger) field.get(instance);
                    new HdfFixedPoint(
                            value.toByteArray(),
                            fixedPointDatatype.getSize(),
                            fixedPointDatatype.isBigEndian(),
                            fixedPointDatatype.isLopad(),
                            fixedPointDatatype.isHipad(),
                            fixedPointDatatype.isSigned(),
                            fixedPointDatatype.getBitOffset(),
                            fixedPointDatatype.getBitPrecision()
                    ).writeValueToByteBuffer(buffer);
                }
            } else {
                ByteBuffer writeBuffer = ByteBuffer.allocate(recordSize * elementsPerRecord).order(buffer.order());
                if (scale > 0) {
                    BigDecimal[] value = (BigDecimal[]) field.get(instance);
                    if (value.length != elementsPerRecord) {
                        throw new IllegalArgumentException("Array size mismatches dimensions: " + value.length + " != " + elementsPerRecord);
                    }
                    for (BigDecimal v : value) {
                        new HdfFixedPoint(
                                v.unscaledValue().toByteArray(),
                                fixedPointDatatype.getSize(),
                                fixedPointDatatype.isBigEndian(),
                                fixedPointDatatype.isLopad(),
                                fixedPointDatatype.isHipad(),
                                fixedPointDatatype.isSigned(),
                                fixedPointDatatype.getBitOffset(),
                                fixedPointDatatype.getBitPrecision()
                        ).writeValueToByteBuffer(writeBuffer);
                    }
                } else {
                    BigInteger[] value = (BigInteger[]) field.get(instance);
                    if (value.length != elementsPerRecord) {
                        throw new IllegalArgumentException("Array size mismatches dimensions: " + value.length + " != " + elementsPerRecord);
                    }
                    for (BigInteger v : value) {
                        new HdfFixedPoint(
                                v.toByteArray(),
                                fixedPointDatatype.getSize(),
                                fixedPointDatatype.isBigEndian(),
                                fixedPointDatatype.isLopad(),
                                fixedPointDatatype.isHipad(),
                                fixedPointDatatype.isSigned(),
                                fixedPointDatatype.getBitOffset(),
                                fixedPointDatatype.getBitPrecision()
                        ).writeValueToByteBuffer(writeBuffer);
                    }
                }
                buffer.put(writeBuffer.array());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error writing instance of " + clazz.getName() + " to ByteBuffer", e);
        }
    }

    /**
     * Returns a sequential Stream of T for reading data from the associated FileChannel.
     * Each element in the stream is an instance of T populated from the HDF5 dataset, starting
     * at the offset provided in the constructor. Use this for ordered, single-threaded processing.
     *
     * @return a sequential Stream of T
     * @throws IllegalStateException if no FileChannel was provided in the constructor
     */
    public Stream<T> stream() {
        if (fileChannel == null) {
            throw new IllegalStateException("Streaming requires a FileChannel; use the appropriate constructor.");
        }
        return StreamSupport.stream(new FixedPointSpliterator(startOffset, endOffset), false);
    }

    /**
     * Returns a parallel Stream of T for reading data from the associated FileChannel.
     * Each element in the stream is an instance of T populated from the HDF5 dataset, starting
     * at the offset provided in the constructor. Use this for multi-threaded processing; the
     * stream will split the dataset across threads for parallel execution.
     *
     * @return a parallel Stream of T
     * @throws IllegalStateException if no FileChannel was provided in the constructor
     */
    public Stream<T> parallelStream() {
        if (fileChannel == null) {
            throw new IllegalStateException("Streaming requires a FileChannel; use the appropriate constructor.");
        }
        return StreamSupport.stream(new FixedPointSpliterator(startOffset, endOffset), true);
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

    private class FixedPointSpliterator implements Spliterator<T> {
        private final long endOffset;
        private long currentOffset;

        FixedPointSpliterator(long startOffset, long endOffset) {
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
                T dataSource = populateFromBuffer(buffer);
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