package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.AllocationRecord;
import org.hdf5javalib.redo.AllocationType;
import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.HdfFileAllocation;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public class HdfLocalHeapData extends AllocationRecord {
    /** The offset to the heap's data segment in the file. */
    private HdfFixedPoint heapContentsOffset;
    /** The size of the heap's data segment. */
    private HdfFixedPoint heapContentsSize;
    /** The offset to the free list within the heap. */
    private HdfFixedPoint freeListOffset;

    private final Map<Integer, HdfLocalHeapDataValue> data;

    /**
     * for reading
     * @param name               name
     * @param heapContentsOffset    heapContentsOffset
     * @param heapContentsSize  heapContentsSize
     * @param data              data
     */
    public HdfLocalHeapData(String name, HdfFixedPoint heapContentsOffset, HdfFixedPoint heapContentsSize, Map<Integer, HdfLocalHeapDataValue> data) {
        super(AllocationType.LOCAL_HEAP, name, heapContentsOffset, heapContentsSize);
        this.data = data;
    }

    /**
     * for writing
     * @param name      name
     * @param offset    offset
     * @param size      size
     * @param hdfDataFile   hdfDataFile
     */
    public HdfLocalHeapData(String name, HdfFixedPoint offset, HdfFixedPoint size, HdfDataFile hdfDataFile) {
        super(AllocationType.LOCAL_HEAP, name, offset, size);
        this.heapContentsSize = size;
        this.heapContentsOffset = offset;
        this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength());
        this.data = new LinkedHashMap<>();
    }

    /**
     * Adds a string to the local heap and returns its offset.
     *
     * @param objectName the string to add to the heap
     * @return the offset in the heap where the string is stored
     */
//    public int addToHeap(HdfString objectName) {
    public int addToHeap(String objectName, HdfDataFile hdfDataFile) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        byte[] objectNameBytes = objectName.getBytes(StandardCharsets.US_ASCII);
        int iFreeListOffset = this.freeListOffset.getInstance(Long.class).intValue();

        int heapSize = heapContentsSize.getInstance(Integer.class);

        // Calculate string size and alignment
        int stringSize = objectNameBytes.length + 1; // Include null terminator
        int alignedStringSize = (stringSize + 7) & ~7; // Pad to 8-byte boundary

        // Determine current heap offset
        int currentOffset = iFreeListOffset == 1 ? heapSize : iFreeListOffset;

        // Calculate initial required space (string only)
        int requiredSpace = alignedStringSize;

        // Check if there's enough space for the string
        if (currentOffset + requiredSpace > heapSize) {
            // Resize heap
            HdfFixedPoint newSize = fileAllocation.expandLocalHeapContents();
//            byte[] newHeapData = new byte[(int) newSize];
//            System.arraycopy(heapData, 0, newHeapData, 0, heapData.getSize().getInstance(Integer.class)); // Copy existing data
            this.heapContentsSize = newSize;
            this.heapContentsOffset = fileAllocation.getCurrentLocalHeapContentsOffset();
//            heapData = new HdfLocalHeapData(heapData.getName(), heapContentsOffset, heapContentsSize, heapData);
//            heapSize = (int) newSize;
        }

        // Check if there's enough space for a free block after the string
//        boolean includeFreeBlock = (heapSize - (currentOffset + alignedStringSize)) >= 16;
//        if (includeFreeBlock) {
//            requiredSpace += 16; // Add 16 bytes for free block
//        }

        // Write string
//        System.arraycopy(objectNameBytes, 0, heapData, currentOffset, objectNameBytes.length);
//        heapData[currentOffset + objectNameBytes.length] = 0; // Null terminator
//        Arrays.fill(heapData, currentOffset + stringSize, currentOffset + alignedStringSize, (byte) 0); // Padding
        data.put(currentOffset, new HdfLocalHeapDataValue(objectName, currentOffset));

        // Update offset
//        int newFreeListOffset = currentOffset + alignedStringSize;

//        // Handle free list
//        if (includeFreeBlock) {
//            // Write free block metadata
//            ByteBuffer buffer = ByteBuffer.wrap(heapData).order(ByteOrder.LITTLE_ENDIAN);
//            buffer.putLong(newFreeListOffset, 1); // Next offset: 1 (last block)
//            buffer.putLong(newFreeListOffset + 8, heapSize - newFreeListOffset); // Remaining space
//            this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(newFreeListOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength());
//        } else {
//            // Set freeListOffset to actual offset unless heap is exactly full
//            if (currentOffset + alignedStringSize == heapSize) {
//                this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(1, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength()); // Heap full, mimic C++ behavior
//            } else {
//                this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(newFreeListOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength()); // Use actual offset
//            }
//        }
//
        return currentOffset;
    }

    public static HdfLocalHeapData readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfFixedPoint dataSegmentSize,
            HdfFixedPoint freeListOffset,
            HdfFixedPoint dataSegmentAddress
    ) throws IOException {

        Map<HdfFixedPoint, HdfLocalHeapDataValue> data = new LinkedHashMap<>();
        fileChannel.position(dataSegmentAddress.getInstance(Long.class));
        // Allocate buffer and read heap data from the file channel
        byte[] heapData = new byte[dataSegmentSize.getInstance(Long.class).intValue()];
        ByteBuffer buffer = ByteBuffer.wrap(heapData);

        fileChannel.read(buffer);
        int iFreeListOffset = freeListOffset.getInstance(Long.class).intValue();
        int iOffset = 0;
        while ( buffer.position() < iFreeListOffset ) {
            int iStart = iOffset;
            // Find the null terminator
            while (iOffset < heapData.length && heapData[iOffset] != 0) {
                iOffset++;
            }
            String objectName = new String(heapData, iStart, iOffset - iStart, StandardCharsets.US_ASCII);
            HdfLocalHeapDataValue value = new HdfLocalHeapDataValue(objectName, iStart);
            data.put(HdfWriteUtils.hdfFixedPointFromValue(iStart, dataSegmentSize.getDatatype()), value);
            iOffset = (iOffset + 7) & ~7;
            buffer.position(iOffset);
        }

        return null;
    }

    public void writeToByteChannel(SeekableByteChannel seekableByteChannel, HdfDataFile hdfDataFile) throws IOException {

//        buffer = ByteBuffer.wrap(heapData);
//        seekableByteChannel.position(hdfDataFile.getFileAllocation().getCurrentLocalHeapContentsOffset());
//        while (buffer.hasRemaining()) {
//            seekableByteChannel.write(buffer);
//        }

    }

    @Override
    public String toString() {
        return "HdfLocalHeapData{" +
                ", dataSegmentSize=" + heapContentsSize +
                ", freeListOffset=" + freeListOffset +
                ", dataSegmentAddress=" + heapContentsOffset +
                ", heapData=" + data +
                "}";
    }

    public HdfFixedPoint getHeapContentsSize() {
        return heapContentsSize;
    }

    public HdfFixedPoint getFreeListOffset() {
        return freeListOffset;
    }

    public HdfFixedPoint getHeapContentsOffset() {
        return heapContentsOffset;
    }

    public String getStringAtOffset(HdfFixedPoint offset) {
        return data.get(offset.getInstance(Long.class).intValue()).getValue();
    }
}
