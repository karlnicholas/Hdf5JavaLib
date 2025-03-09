package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class HdfWriteUtils {

    /**
     * Writes an `HdfFixedPoint` value to the `ByteBuffer' accounting for endian-ness.
     * If undefined, fills with 0xFF.
     */
    public static void writeFixedPointToBuffer(ByteBuffer buffer, HdfFixedPoint value) {
        int size = value.getSizeMessageData();
        byte[] bytesToWrite = new byte[size];

        if (value.isUndefined()) {
            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
        } else {
            byte[] valueBytes = value.getBytes();
            int copySize = Math.min(valueBytes.length, size);

            // Store in **little-endian format** by reversing byte order
            if ( value.getDatatype().isBigEndian() && buffer.order() == ByteOrder.BIG_ENDIAN
            || !value.getDatatype().isBigEndian() && buffer.order() == ByteOrder.LITTLE_ENDIAN) {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[i];
                }
            } else {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[copySize - 1 - i];
                }
            }
        }

        buffer.put(bytesToWrite);
    }

    public static void writeFixedPointToBuffer(ByteBuffer buffer, BigInteger value, int size) {
//        short size = value.getSizeMessageData();
        byte[] bytesToWrite = new byte[size];

//        if (value.isUndefined()) {
//            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
//        } else {
            byte[] valueBytes = value.toByteArray();
            int copySize = Math.min(valueBytes.length, size);

            // Store in **little-endian format** by reversing byte order
            if ( buffer.order() == ByteOrder.BIG_ENDIAN ) {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[i];
                }
            } else {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[copySize - 1 - i];
                }
            }
//        }

        buffer.put(bytesToWrite);
    }

    public static void writeBigIntegerAsHdfFixedPoint(BigInteger value, HdfDatatype datatype, ByteBuffer buffer) {
        FixedPointDatatype fixedPointDatatype = (FixedPointDatatype)datatype;
        HdfFixedPoint fixedPoint = new HdfFixedPoint(value.toByteArray(), fixedPointDatatype);
        fixedPoint.writeValueToByteBuffer(buffer);
    }

    public static void writeBigDecimalAsHdfFixedPoint(BigDecimal value, HdfDatatype datatype, ByteBuffer buffer) {
        FixedPointDatatype fixedPointDatatype = (FixedPointDatatype)datatype;
        HdfFixedPoint fixedPoint = new HdfFixedPoint(value.unscaledValue().toByteArray(), fixedPointDatatype);
        fixedPoint.writeValueToByteBuffer(buffer);
    }
}
