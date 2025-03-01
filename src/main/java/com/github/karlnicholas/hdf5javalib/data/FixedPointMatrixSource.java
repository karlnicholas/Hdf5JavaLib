package com.github.karlnicholas.hdf5javalib.data;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.message.DataspaceMessage;
import com.github.karlnicholas.hdf5javalib.message.DatatypeMessage;
import com.github.karlnicholas.hdf5javalib.message.datatype.FixedPointDatatype;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FixedPointMatrixSource<T> {
    private final Class<T> clazz;
    private final HdfObjectHeaderPrefixV1 headerPrefixV1;
    private final Field field;
    private final int recordSize;
    private final int readsAvailable;
    private final int dimension;
    private final FixedPointDatatype fixedPointDatatype;
    private final int scale;

    public FixedPointMatrixSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, Class<T> clazz) {
        this.clazz = clazz;
        this.headerPrefixV1 = headerPrefixV1;
        recordSize = headerPrefixV1.findMessageByType(DatatypeMessage.class).orElseThrow().getHdfDatatype().getSize();
        HdfFixedPoint[] dimensions = headerPrefixV1.findMessageByType(DataspaceMessage.class).orElseThrow().getDimensions();
        readsAvailable = dimensions[0].toBigInteger().intValue();
        dimension = dimensions[1].toBigInteger().intValue();
        fixedPointDatatype = (FixedPointDatatype) headerPrefixV1.findMessageByType(DatatypeMessage.class).orElseThrow().getHdfDatatype();
        this.scale = scale;
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
            if ( scale > 0 ) {
                BigDecimal[] data = new BigDecimal[dimension];
                for(int i = 0; i < dimension; i++) {
                    HdfFixedPoint fp = fixedPointDatatype.getInstance(buffer);
                    data[i] = fp.toBigDecimal(scale);
                }
                field.set(instance, data);
            } else {
                BigInteger[] data = new BigInteger[dimension];
                for(int i = 0; i < dimension; i++) {
                    HdfFixedPoint fp = fixedPointDatatype.getInstance(buffer);
                    data[i] = fp.toBigInteger();
                }
                field.set(instance, data);
            }
            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error creating and populating instance of " + clazz.getName(), e);
        }
    }
    /**
     * Writes the given instance of T into the provided ByteBuffer.
     */
//    public void writeToBuffer(T instance, ByteBuffer buffer) {
//        try {
//            BigInteger bigIntValue = (BigInteger) field.get(instance);
//
//            // Convert BigInteger to bytes and write to buffer
//            new HdfFixedPoint(bigIntValue.toByteArray(), fixedPointDatatype.getSize(), fixedPointDatatype.isBigEndian(), fixedPointDatatype.isLopad(), fixedPointDatatype.isHipad(), fixedPointDatatype.isSigned(), fixedPointDatatype.getBitOffset(), fixedPointDatatype.getBitPrecision())
//                    .writeValueToByteBuffer(buffer);
//            // Add more type handling as needed
//
//        } catch (Exception e) {
//            throw new RuntimeException("Error writing instance of " + clazz.getName() + " to ByteBuffer", e);
//        }
//    }

    public long getSizeForReadBuffer() {
        return recordSize * dimension;
    }

    public long getNumberOfReadsAvailable() {
        return readsAvailable;
    }
}
