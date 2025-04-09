package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

@Getter
public class HdfLocalHeapContents {
    private final byte[] heapData;
    private final HdfDataFile hdfDataFile;

    /**
     * Constructor for application-defined heap data.
     *
     * @param heapData The heap data as a byte array.
     * @param hdfDataFile
     */
    public HdfLocalHeapContents(byte[] heapData, HdfDataFile hdfDataFile) {
        this.heapData = heapData;
        this.hdfDataFile = hdfDataFile;
    }

    /**
     * @param fileChannel fileChannel
     * @param dataSize size
     * @return HdfLocalHeapContents
     * @throws IOException ioexception
     */
    public static HdfLocalHeapContents readFromFileChannel(FileChannel fileChannel, int dataSize, HdfDataFile hdfDataFile) throws IOException {
        // Allocate buffer and read heap data from the file channel
        byte[] heapData = new byte[dataSize];
        ByteBuffer buffer = ByteBuffer.wrap(heapData);

        fileChannel.read(buffer);

        return new HdfLocalHeapContents(heapData, hdfDataFile);
    }

    /**
     * Parses the next null-terminated string from the heap data.
     *
     * @return The next string, or null if no more strings are available.
     */
    public HdfString parseStringAtOffset(HdfFixedPoint offset) {
        long iOffset = offset.getInstance(Long.class);
        if (iOffset >= heapData.length) {
            return null; // End of heap data
        }

        long start = iOffset;

        // Find the null terminator
        while (iOffset < heapData.length && heapData[(int) iOffset] != 0) {
            iOffset++;
        }

        // Extract the string

        return new HdfString(Arrays.copyOfRange(heapData, (int) start, (int) iOffset), new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), (int) (iOffset - start)));
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        buffer.position((int)(fileAllocation.getCurrentLocalHeapContentsOffset() - fileAllocation.getRootGroupOffset()));
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
