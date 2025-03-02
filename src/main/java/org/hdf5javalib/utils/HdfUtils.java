package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.nio.ByteBuffer;

public class HdfUtils {

    /**
     * Writes an `HdfFixedPoint` value to the `ByteBuffer` in **little-endian format**.
     * If undefined, fills with 0xFF.
     */
    public static void writeFixedPointToBuffer(ByteBuffer buffer, HdfFixedPoint value) {
        short size = value.getSizeMessageData();
        byte[] bytesToWrite = new byte[size];

//        if (value.isUndefined()) {
//            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
//        } else {
//            byte[] valueBytes = value.toBigInteger().toByteArray();
//            int copySize = Math.min(valueBytes.length, size);
//
//            // Store in **little-endian format** by reversing byte order
//            for (int i = 0; i < copySize; i++) {
//                bytesToWrite[i] = valueBytes[copySize - 1 - i];
//            }
//        }

        buffer.put(bytesToWrite);
    }

}
