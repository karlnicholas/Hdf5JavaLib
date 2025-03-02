package org.hdf5javalib.datasource;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A data source for reading and writing typed fixed-point data from HDF5 files into objects of type T.
 * Supports streaming and bulk reading into T arrays, mapping dataset values to a specified field in T.
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>
 * FixedPointTypedDataSource<Temperature> dataSource = new FixedPointTypedDataSource<>(header, "temp", 0, Temperature.class, fileChannel, dataAddress);
 * Temperature[] temps = dataSource.readAll();
 * dataSource.stream().forEach(t -> System.out.println(t.temperature));
 * </pre>
 */
public class FixedPointTypedDataSource<T> extends AbstractFixedPointDataSource<T> {
    private final Class<T> clazz;
    private final Field field;

    /**
     * Constructs a FixedPointTypedDataSource for manual buffer operations without streaming capabilities.
     *
     * @param headerPrefixV1 the HDF5 object header prefix
     * @param name the name of the field in T to map to the dataset
     * @param scale the scale for BigDecimal values; 0 for BigInteger
     * @param clazz the class of the target object T
     * @throws IllegalStateException if metadata is missing
     * @throws IllegalArgumentException if field is not found or type is incompatible
     */
    public FixedPointTypedDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz) {
        this(headerPrefixV1, name, scale, clazz, null, 0);
    }

    /**
     * Constructs a FixedPointTypedDataSource for buffer operations and streaming from an HDF5 file.
     *
     * @param headerPrefixV1 the HDF5 object header prefix
     * @param name the name of the field in T to map to the dataset
     * @param scale the scale for BigDecimal values; 0 for BigInteger
     * @param clazz the class of the target object T
     * @param fileChannel the FileChannel for streaming
     * @param startOffset the byte offset where the dataset begins
     * @throws IllegalStateException if metadata is missing
     * @throws IllegalArgumentException if field is not found or type is incompatible
     */
    public FixedPointTypedDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz, FileChannel fileChannel, long startOffset) {
        super(headerPrefixV1, scale, fileChannel, startOffset);
        this.clazz = clazz;

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
    }

    /**
     * Populates a new instance of T with fixed-point data from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the dataset data
     * @return a new instance of T with the field populated
     * @throws RuntimeException if instantiation or field setting fails
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
     *
     * @param instance the object T to write
     * @param buffer the ByteBuffer to write into
     * @throws RuntimeException if field access or writing fails
     * @throws IllegalArgumentException if matrix array size mismatches dimensions
     */
    public void writeToBuffer(T instance, ByteBuffer buffer) {
        try {
            if (elementsPerRecord == 1) {
                if (scale > 0) {
                    BigDecimal value = (BigDecimal) field.get(instance);
                    new HdfFixedPoint(
                            HdfFixedPoint.toSizedByteArray(value.unscaledValue(),fixedPointDatatype.getSize(), fixedPointDatatype.isBigEndian()),
                            fixedPointDatatype.getSize(),
                            fixedPointDatatype.isBigEndian(),
                            fixedPointDatatype.isLoPad(),
                            fixedPointDatatype.isHiPad(),
                            fixedPointDatatype.isSigned(),
                            fixedPointDatatype.getBitOffset(),
                            fixedPointDatatype.getBitPrecision()
                    ).writeValueToByteBuffer(buffer);
                } else {
                    BigInteger value = (BigInteger) field.get(instance);
                    new HdfFixedPoint(
                            value,
                            fixedPointDatatype.getSize(),
                            fixedPointDatatype.isSigned(),
                            fixedPointDatatype.isBigEndian()
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
                                HdfFixedPoint.toSizedByteArray(v.unscaledValue(), fixedPointDatatype.getSize(), fixedPointDatatype.isBigEndian()),
                                fixedPointDatatype.getSize(),
                                fixedPointDatatype.isBigEndian(),
                                fixedPointDatatype.isLoPad(),
                                fixedPointDatatype.isHiPad(),
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
                                v,
                                fixedPointDatatype.getSize(),
                                fixedPointDatatype.isSigned(),
                                fixedPointDatatype.isBigEndian()
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
     * Reads the entire dataset into an array of T in memory.
     *
     * @return an array of T containing all records
     * @throws IllegalStateException if no FileChannel was provided
     */
    public T[] readAll() {
        if (fileChannel == null) {
            throw new IllegalStateException("Reading all data requires a FileChannel; use the appropriate constructor.");
        }
        @SuppressWarnings("unchecked")
        T[] result = (T[]) java.lang.reflect.Array.newInstance(clazz, (int) readsAvailable);
        return stream().toArray(size -> result);
    }

    /**
     * Returns a sequential Stream of T for reading data from the FileChannel.
     *
     * @return a sequential Stream of T
     * @throws IllegalStateException if no FileChannel was provided
     */
    @Override
    public Stream<T> stream() {
        if (fileChannel == null) {
            throw new IllegalStateException("Streaming requires a FileChannel; use the appropriate constructor.");
        }
        return StreamSupport.stream(new FixedPointSpliterator(startOffset, endOffset), false);
    }

    /**
     * Returns a parallel Stream of T for reading data from the FileChannel.
     *
     * @return a parallel Stream of T
     * @throws IllegalStateException if no FileChannel was provided
     */
    @Override
    public Stream<T> parallelStream() {
        if (fileChannel == null) {
            throw new IllegalStateException("Streaming requires a FileChannel; use the appropriate constructor.");
        }
        return StreamSupport.stream(new FixedPointSpliterator(startOffset, endOffset), true);
    }

    @Override
    protected T populateFromBufferRaw(ByteBuffer buffer) {
        return populateFromBuffer(buffer);
    }
}