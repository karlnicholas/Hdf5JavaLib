package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class HdfLocalHeapContents {
    private final byte[] heapData;

    /**
     * Constructor for application-defined heap data.
     *
     * @param heapData The heap data as a byte array.
     */
    public HdfLocalHeapContents(byte[] heapData) {
        this.heapData = heapData;
    }

    /**
     * @param fileChannel fileChannel
     * @param dataSize size
     * @return HdfLocalHeapContents
     * @throws IOException
     */
    public static HdfLocalHeapContents readFromFileChannel(FileChannel fileChannel, int dataSize) throws IOException {
        // Allocate buffer and read heap data from the file channel
        byte[] heapData = new byte[dataSize];
        ByteBuffer buffer = ByteBuffer.wrap(heapData);

        fileChannel.read(buffer);

        return new HdfLocalHeapContents(heapData);
    }

    /**
     * Parses the next null-terminated string from the heap data.
     *
     * @return The next string, or null if no more strings are available.
     */
    public HdfString parseStringAtOffset(HdfFixedPoint offset) {
        int iOffset = offset.getBigIntegerValue().intValue();
        if (iOffset >= heapData.length) {
            return null; // End of heap data
        }

        int start = iOffset;

        // Find the null terminator
        while (iOffset < heapData.length && heapData[iOffset] != 0) {
            iOffset++;
        }

        // Extract the string
        HdfString result = new HdfString(Arrays.copyOfRange(heapData, start, iOffset), false, false);

        return result;
    }

    /**
     * Parses the next 64-bit fixed-point value from the heap data.
     *
     * @return The next fixed-point value, or null if no more data is available.
     */
//    public HdfFixedPoint parseNextFixedPoint() {
//        int fixedPointSize = 8; // Fixed-point size in bytes for 64-bit values
//        if (currentIndex + fixedPointSize > heapData.length) {
//            return null; // Not enough data remaining
//        }
//
//        // Create a fixed-point object from the current position
//        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(
//                ByteBuffer.wrap(heapData, currentIndex, fixedPointSize).order(java.nio.ByteOrder.LITTLE_ENDIAN),
//                fixedPointSize * 8,
//                false
//        );
//
//        // Advance the index by the size of the fixed-point value
//        currentIndex += fixedPointSize;
//
//        return fixedPoint;
//    }

    /**
     * Provides a simple string representation of the heap contents.
     *
     * @return A string containing the heap size and current parsing index.
     */
    @Override
    public String toString() {
        return "HdfLocalHeapContents{" +
                "heapDataSize=" + heapData.length +
                ", heapData=" + Arrays.toString(Arrays.copyOf(heapData, Math.min(heapData.length, 32))) +
                "... (truncated)" +
                '}';
    }
}
