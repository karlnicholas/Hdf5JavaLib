package com.github.karlnicholas.hdf5javalib.data;

import com.github.karlnicholas.hdf5javalib.message.datatype.FixedPointDatatype;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FixedPointVectorSource<T> {
    private final Class<T> clazz;
    private final FixedPointDatatype fixedPointDatatype;
    private final Field field;

    public FixedPointVectorSource(FixedPointDatatype fixedPointDatatype, String name, Class<T> clazz) {
        this.clazz = clazz;
        this.fixedPointDatatype = fixedPointDatatype;
        // Parse fields and map them to CompoundDatatype members
        Field fieldToSet = null;
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            if( field.getName().equals(name)) {
                fieldToSet = field;
                break;
            }
        }
        this.field = fieldToSet;
    }

    /**
     * Populates a new instance of T with data from the buffer.
     */
    public T populateFromBuffer(ByteBuffer buffer) {
        try {
            // Create an instance of T
            T instance = clazz.getDeclaredConstructor().newInstance();
            BigInteger value = fixedPointDatatype.getInstance(buffer).toBigInteger();
            field.set(instance, value);
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
            BigInteger bigIntValue = (BigInteger) field.get(instance);

            // Convert BigInteger to bytes and write to buffer
            new HdfFixedPoint(bigIntValue.toByteArray(), fixedPointDatatype.getSize(), fixedPointDatatype.isBigEndian(), fixedPointDatatype.isLopad(), fixedPointDatatype.isHipad(), fixedPointDatatype.isSigned(), fixedPointDatatype.getBitOffset(), fixedPointDatatype.getBitPrecision())
                    .writeValueToByteBuffer(buffer);
            // Add more type handling as needed

        } catch (Exception e) {
            throw new RuntimeException("Error writing instance of " + clazz.getName() + " to ByteBuffer", e);
        }
    }
}
