package com.github.karlnicholas.hdf5javalib.data;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.message.DataspaceMessage;
import com.github.karlnicholas.hdf5javalib.message.DatatypeMessage;
import com.github.karlnicholas.hdf5javalib.message.datatype.FixedPointDatatype;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FixedPointDataSource<T> {
    private final Class<T> clazz;
    private final HdfObjectHeaderPrefixV1 headerPrefixV1;
    private final Field field;
    private final int recordSize;
    private final int readsAvailable;
    private final int elementsPerRecord; // 1 for vector, dimension for matrix
    private final FixedPointDatatype fixedPointDatatype;
    private final int scale;

    public FixedPointDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz) {
        this.clazz = clazz;
        this.headerPrefixV1 = headerPrefixV1;
        this.scale = scale;

        // Extract metadata from header
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

        // Determine elementsPerRecord based on dimensionality
        if (dimensions.length == 1) {
            elementsPerRecord = 1; // Vector case
        } else if (dimensions.length == 2) {
            elementsPerRecord = dimensions[1].toBigInteger().intValue(); // Matrix case
        } else {
            throw new IllegalArgumentException("Unsupported dimensionality: " + dimensions.length);
        }

        // Find the field by name
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

        // Validate field type compatibility
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
     * Populates a new instance of T with data from the buffer.
     */
    public T populateFromBuffer(ByteBuffer buffer) {
        try {
            T instance = clazz.getDeclaredConstructor().newInstance();
            if (elementsPerRecord == 1) {
                // Vector case: single value
                if (scale > 0) {
                    BigDecimal value = fixedPointDatatype.getInstance(buffer).toBigDecimal(scale);
                    field.set(instance, value);
                } else {
                    BigInteger value = fixedPointDatatype.getInstance(buffer).toBigInteger();
                    field.set(instance, value);
                }
            } else {
                // Matrix case: array of values
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
     * Writes the given instance of T into the provided ByteBuffer.
     */
    public void writeToBuffer(T instance, ByteBuffer buffer) {
        try {
            if (elementsPerRecord == 1) {
                // Vector case: single value
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
                // Matrix case: array of values
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
     * Returns the size of the buffer needed for reading one record.
     */
    public long getSizeForReadBuffer() {
        return (long) recordSize * elementsPerRecord;
    }

    /**
     * Returns the number of records available to read.
     */
    public long getNumberOfReadsAvailable() {
        return readsAvailable;
    }
}