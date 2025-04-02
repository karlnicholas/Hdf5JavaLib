package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfBufferAllocation;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

@Getter
public class HdfLocalHeap {
    private final String signature;
    private final int version;
    private HdfFixedPoint dataSegmentSize;
    private HdfFixedPoint freeListOffset;
    private HdfFixedPoint dataSegmentAddress;

    public HdfLocalHeap(String signature, int version, HdfFixedPoint dataSegmentSize,
                        HdfFixedPoint freeListOffset, HdfFixedPoint dataSegmentAddress) {
        this.signature = signature;
        this.version = version;
        this.dataSegmentSize = dataSegmentSize;
        this.freeListOffset = freeListOffset;
        this.dataSegmentAddress = dataSegmentAddress;
    }

    public HdfLocalHeap(HdfFixedPoint dataSegmentSize, HdfFixedPoint dataSegmentAddress) {
        this("HEAP", 0, dataSegmentSize, HdfFixedPoint.of(0), dataSegmentAddress);
    }

    public int addToHeap(HdfString objectName, HdfLocalHeapContents localHeapContents,
                         HdfBufferAllocation bufferAllocation) {
        byte[] objectNameBytes = objectName.getBytes();
        int freeListOffset = this.freeListOffset.getInstance(Long.class).intValue();
        byte[] heapData = localHeapContents.getHeapData();

        // Extract free space size
        int freeBlockSize = ByteBuffer.wrap(heapData, freeListOffset + 8, 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();

        // Check if there’s enough space; if not, request resize from HdfBufferAllocation
        int requiredSpace = objectNameBytes.length + ((objectNameBytes.length == 0) ? 8 :
                ((objectNameBytes.length + 7) & ~7) - objectNameBytes.length + 16);
        if (requiredSpace > freeBlockSize) {
            // Call resizeHeap, assuming HdfBufferAllocation knows current size
            HdfBufferAllocation.HeapResizeResult result = bufferAllocation.resizeHeap(
                    localHeapContents, freeListOffset, requiredSpace);
            localHeapContents = result.getNewContents();
            this.dataSegmentSize = HdfFixedPoint.of(result.getNewContents().getHeapData().length);
            this.dataSegmentAddress = result.getNewAddress();
            heapData = localHeapContents.getHeapData(); // Refresh heapData
            freeBlockSize = ByteBuffer.wrap(heapData, freeListOffset + 8, 8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt(); // Refresh freeBlockSize
        }

        // Copy the new objectName
        System.arraycopy(objectNameBytes, 0, heapData, freeListOffset, objectNameBytes.length);

        // Align freeListOffset
        int newFreeListOffset = (objectNameBytes.length == 0) ? freeListOffset + 8 :
                (freeListOffset + objectNameBytes.length + 7) & ~7;
        Arrays.fill(heapData, freeListOffset + objectNameBytes.length, newFreeListOffset, (byte) 0);

        // Update remaining free space
        int remainingFreeSpace = freeBlockSize - (newFreeListOffset - freeListOffset);
        this.freeListOffset = HdfFixedPoint.of(newFreeListOffset);

        // Write free block metadata
        ByteBuffer buffer = ByteBuffer.wrap(heapData).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(newFreeListOffset, 1);
        buffer.putLong(newFreeListOffset + 8, remainingFreeSpace);
        return freeListOffset;
    }
}