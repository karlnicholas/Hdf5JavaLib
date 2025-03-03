package org.hdf5javalib.datasource;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
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
public class TypedDataSource<T> extends AbstractDataClassDataSource<T> {
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
    public TypedDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz) {
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
    public TypedDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz, FileChannel fileChannel, long startOffset) {
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

//        Class<?> fieldType = field.getType();
//        if (elementsPerRecord == 1) {
//            if (!(fieldType.equals(BigDecimal.class) || fieldType.equals(BigInteger.class))) {
//                throw new IllegalArgumentException("Vector field must be BigDecimal or BigInteger, got " + fieldType);
//            }
//        } else {
//            if (!(fieldType.isArray() &&
//                    (fieldType.getComponentType().equals(BigDecimal.class) ||
//                            fieldType.getComponentType().equals(BigInteger.class)))) {
//                throw new IllegalArgumentException("Matrix field must be BigDecimal[] or BigInteger[], got " + fieldType);
//            }
//        }
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
            Class<?> fieldType = field.getType();
            boolean isArray = elementsPerRecord > 1;

            // Validate field type compatibility
            if (isArray && !fieldType.isArray()) {
                throw new IllegalArgumentException("Field " + field.getName() + " must be an array for multi-element records");
            }
            Class<?> componentType = isArray ? fieldType.getComponentType() : fieldType;

            // Handle data population
            if (StringDatatype.class.isInstance(datatype)) {
                if (componentType != String.class) {
                    throw new IllegalArgumentException("Field " + field.getName() + " must be String or String[] for StringDatatype");
                }
                if (isArray) {
                    String[] data = new String[elementsPerRecord];
                    for (int i = 0; i < elementsPerRecord; i++) {
                        data[i] = ((HdfString) datatype.getInstance(buffer)).getValue();
                    }
                    field.set(instance, data);
                } else {
                    String value = ((HdfString) datatype.getInstance(buffer)).getValue();
                    field.set(instance, value);
                }
            } else if (FixedPointDatatype.class.isInstance(datatype)) {
                if (componentType != BigInteger.class && componentType != BigDecimal.class) {
                    throw new IllegalArgumentException("Field " + field.getName() + " must be BigInteger/BigDecimal or their arrays for FixedPointDatatype");
                }
                if (isArray) {
                    if (componentType == BigInteger.class) {
                        BigInteger[] data = new BigInteger[elementsPerRecord];
                        for (int i = 0; i < elementsPerRecord; i++) {
                            data[i] = ((HdfFixedPoint) datatype.getInstance(buffer)).toBigInteger();
                        }
                        field.set(instance, data);
                    } else {
                        BigDecimal[] data = new BigDecimal[elementsPerRecord];
                        for (int i = 0; i < elementsPerRecord; i++) {
                            data[i] = ((HdfFixedPoint) datatype.getInstance(buffer)).toBigDecimal(scale);
                        }
                        field.set(instance, data);
                    }
                } else {
                    if (componentType == BigInteger.class) {
                        BigInteger value = ((HdfFixedPoint) datatype.getInstance(buffer)).toBigInteger();
                        field.set(instance, value);
                    } else {
                        BigDecimal value = ((HdfFixedPoint) datatype.getInstance(buffer)).toBigDecimal(scale);
                        field.set(instance, value);
                    }
                }
            } else {
                throw new IllegalArgumentException("Unsupported datatype: " + datatype.getClass().getName());
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
     * @throws IllegalArgumentException if field type or array size is incompatible
     */
    public void writeToBuffer(T instance, ByteBuffer buffer) {
        try {
            Class<?> fieldType = field.getType();
            boolean isArray = elementsPerRecord > 1;

            // Validate field type compatibility
            if (isArray && !fieldType.isArray()) {
                throw new IllegalArgumentException("Field " + field.getName() + " must be an array for multi-element records");
            }
            Class<?> componentType = isArray ? fieldType.getComponentType() : fieldType;

            if (StringDatatype.class.isInstance(datatype)) {
                StringDatatype stringDt = (StringDatatype) datatype;
                if (componentType != String.class) {
                    throw new IllegalArgumentException("Field " + field.getName() + " must be String or String[] for StringDatatype");
                }
                BitSet classBitField = stringDt.getClassBitField();
                Charset charset = getCharsetFromCharacterSet(stringDt.getCharacterSet());
                int stringSize = stringDt.getSize();
                StringDatatype.PaddingType paddingType = stringDt.getPaddingType();

                if (isArray) {
                    String[] values = (String[]) field.get(instance);
                    if (values.length != elementsPerRecord) {
                        throw new IllegalArgumentException("Array size mismatches dimensions: " + values.length + " != " + elementsPerRecord);
                    }
                    for (String value : values) {
                        writeStringValue(value, charset, stringSize, paddingType, classBitField, buffer);
                    }
                } else {
                    String value = (String) field.get(instance);
                    writeStringValue(value, charset, stringSize, paddingType, classBitField, buffer);
                }
            } else if (FixedPointDatatype.class.isInstance(datatype)) {
                FixedPointDatatype fpDt = (FixedPointDatatype) datatype;
                if (componentType != BigInteger.class && componentType != BigDecimal.class) {
                    throw new IllegalArgumentException("Field " + field.getName() + " must be BigInteger/BigDecimal or their arrays for FixedPointDatatype");
                }
                if (isArray) {
                    if (componentType == BigInteger.class) {
                        BigInteger[] values = (BigInteger[]) field.get(instance);
                        if (values.length != elementsPerRecord) {
                            throw new IllegalArgumentException("Array size mismatches dimensions: " + values.length + " != " + elementsPerRecord);
                        }
                        for (BigInteger v : values) {
                            new HdfFixedPoint(v, fpDt.getSize(), fpDt.isSigned(), fpDt.isBigEndian())
                                    .writeValueToByteBuffer(buffer);
                        }
                    } else {
                        BigDecimal[] values = (BigDecimal[]) field.get(instance);
                        if (values.length != elementsPerRecord) {
                            throw new IllegalArgumentException("Array size mismatches dimensions: " + values.length + " != " + elementsPerRecord);
                        }
                        for (BigDecimal v : values) {
                            new HdfFixedPoint(
                                    HdfFixedPoint.toSizedByteArray(v.unscaledValue(), fpDt.getSize(), fpDt.isBigEndian()),
                                    fpDt.getSize(),
                                    fpDt.isBigEndian(),
                                    fpDt.isLoPad(),
                                    fpDt.isHiPad(),
                                    fpDt.isSigned(),
                                    fpDt.getBitOffset(),
                                    fpDt.getBitPrecision()
                            ).writeValueToByteBuffer(buffer);
                        }
                    }
                } else {
                    if (componentType == BigInteger.class) {
                        BigInteger value = (BigInteger) field.get(instance);
                        new HdfFixedPoint(value, fpDt.getSize(), fpDt.isSigned(), fpDt.isBigEndian())
                                .writeValueToByteBuffer(buffer);
                    } else {
                        BigDecimal value = (BigDecimal) field.get(instance);
                        new HdfFixedPoint(
                                HdfFixedPoint.toSizedByteArray(value.unscaledValue(), fpDt.getSize(), fpDt.isBigEndian()),
                                fpDt.getSize(),
                                fpDt.isBigEndian(),
                                fpDt.isLoPad(),
                                fpDt.isHiPad(),
                                fpDt.isSigned(),
                                fpDt.getBitOffset(),
                                fpDt.getBitPrecision()
                        ).writeValueToByteBuffer(buffer);
                    }
                }
            } else {
                throw new IllegalArgumentException("Unsupported datatype: " + datatype.getClass().getName());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error writing instance of " + clazz.getName() + " to ByteBuffer", e);
        }
    }

    // Helper method to map CharacterSet to Charset
    private Charset getCharsetFromCharacterSet(StringDatatype.CharacterSet charSet) {
        switch (charSet) {
            case ASCII:
                return StandardCharsets.US_ASCII;
            case UTF8:
                return StandardCharsets.UTF_8;
            default:
                throw new IllegalArgumentException("Unsupported character set: " + charSet.getName());
        }
    }

    // Helper method to write a string value with padding
    private void writeStringValue(String value, Charset charset, int size, StringDatatype.PaddingType paddingType, BitSet classBitField, ByteBuffer buffer) {
        byte[] bytes = value.getBytes(charset);
        byte[] paddedBytes = new byte[size];
        int copyLength = Math.min(bytes.length, size);
        System.arraycopy(bytes, 0, paddedBytes, 0, copyLength);

        // Apply padding based on PaddingType
        if (copyLength < size) {
            switch (paddingType) {
                case NULL_TERMINATE:
                case NULL_PAD:
                    // Already zero-filled by new byte[size], just ensure null terminator if needed
                    break;
                case SPACE_PAD:
                    Arrays.fill(paddedBytes, copyLength, size, (byte) ' ');
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported padding type: " + paddingType.getName());
            }
        }

        new HdfString(paddedBytes, classBitField).writeValueToByteBuffer(buffer);
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
        return StreamSupport.stream(new DataClassSpliterator(startOffset, endOffset), false);
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
        return StreamSupport.stream(new DataClassSpliterator(startOffset, endOffset), true);
    }

    @Override
    protected T populateFromBufferRaw(ByteBuffer buffer) {
        return populateFromBuffer(buffer);
    }
}