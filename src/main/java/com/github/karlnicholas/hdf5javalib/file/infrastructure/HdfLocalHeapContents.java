package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.dataclass.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.dataclass.HdfString;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

@Getter
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
        int iOffset = offset.toBigInteger().intValue();
        if (iOffset >= heapData.length) {
            return null; // End of heap data
        }

        int start = iOffset;

        // Find the null terminator
        while (iOffset < heapData.length && heapData[iOffset] != 0) {
            iOffset++;
        }

        // Extract the string
        HdfString result = new HdfString(Arrays.copyOfRange(heapData, start, iOffset), StringDatatype.getStringTypeBitSet(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII));

        return result;
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
        buffer.put(heapData);
    }

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
