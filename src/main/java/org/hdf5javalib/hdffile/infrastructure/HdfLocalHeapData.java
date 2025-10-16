package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.LinkedHashMap;
import java.util.Map;

public class HdfLocalHeapData {
    /**
     * The offset to the heap's data segment in the file.
     */
    private HdfFixedPoint heapContentsOffset;
    /**
     * The size of the heap's data segment.
     */
    private HdfFixedPoint heapContentsSize;
    /**
     * The offset to the free list within the heap.
     */
    private final HdfFixedPoint freeListOffset;
    /**
     * Map of localHeap data by offsets
     */
    private final Map<HdfFixedPoint, HdfLocalHeapDataValue> data;

    /**
     * for reading
     *
     * @param heapContentsOffset heapContentsOffset
     * @param heapContentsSize   heapContentsSize
     * @param data               data
     */
    public HdfLocalHeapData(
            HdfFixedPoint heapContentsOffset,
            HdfFixedPoint heapContentsSize,
            HdfFixedPoint freeListOffset,
            Map<HdfFixedPoint, HdfLocalHeapDataValue> data
    ) {
        this.heapContentsSize = heapContentsSize;
        this.freeListOffset = freeListOffset;
        this.data = data;
    }

    /**
     * for writing
     *
     * @param offset      offset
     * @param size        size
     * @param hdfDataFile hdfDataFile
     */
    public HdfLocalHeapData(HdfFixedPoint offset, HdfFixedPoint size, HdfDataFile hdfDataFile) {
        this.heapContentsSize = size;
        this.heapContentsOffset = offset;
        this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength());
        this.data = new LinkedHashMap<>();
    }

    /**
     * Adds a string to the local heap and returns its offset.
     *
     * @param objectName the string to add to the heap
     * @return the offset in the heap where the string is stored
     */

    public HdfFixedPoint addToHeap(String objectName, HdfDataFile hdfDataFile) {
        return null;
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
        HdfLocalHeapDataValue x = data.get(offset);
        return x.value();
    }
}
