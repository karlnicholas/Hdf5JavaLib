package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HdfWriteUtils {

    /**
     * Writes an `HdfFixedPoint` value to the `ByteBuffer' accounting for endian-ness.
     * If undefined, fills with 0xFF.
     */
    public static void writeFixedPointToBuffer(ByteBuffer buffer, HdfFixedPoint value) {
        short size = value.getSizeMessageData();
        byte[] bytesToWrite = new byte[size];

        if (value.isUndefined()) {
            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
        } else {
            byte[] valueBytes = value.toBigInteger().toByteArray();
            int copySize = Math.min(valueBytes.length, size);

            // Store in **little-endian format** by reversing byte order
            if ( value.isBigEndian()) {
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

}
