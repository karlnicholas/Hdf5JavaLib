package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfFileAllocation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class HdfLocalHeap {
    private final String signature;
    private final int version;
    private HdfFixedPoint heapContentsSize;
    private HdfFixedPoint freeListOffset;
    private HdfFixedPoint heapContentsOffset;
    private final HdfDataFile hdfDataFile;

    public static class Pair<T, U> {
        private final T first;
        private final U second;

        public Pair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }
    }

    public HdfLocalHeap(String signature, int version, HdfFixedPoint heapContentsSize,
                        HdfFixedPoint freeListOffset, HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile) {
        this.signature = signature;
        this.version = version;
        this.heapContentsSize = heapContentsSize;
        this.freeListOffset = freeListOffset;
        this.heapContentsOffset = heapContentsOffset;
        this.hdfDataFile = hdfDataFile;
    }

    public HdfLocalHeap(HdfFixedPoint heapContentsSize, HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile) {
        this("HEAP", 0, heapContentsSize, HdfFixedPoint.of(0), heapContentsOffset, hdfDataFile);
    }

    public Pair<HdfLocalHeapContents, Integer> addToHeap(HdfString objectName, HdfLocalHeapContents localHeapContents) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        byte[] objectNameBytes = objectName.getBytes();
        int freeListOffset = this.freeListOffset.getInstance(Long.class).intValue();
        byte[] heapData = localHeapContents.getHeapData();
        int heapSize = heapData.length;

        // Calculate string size and alignment
        int stringSize = objectNameBytes.length + 1; // Include null terminator
        int alignedStringSize = (stringSize + 7) & ~7; // Pad to 8-byte boundary

        // Determine current heap offset
        int currentOffset = freeListOffset == 1 ? heapSize : freeListOffset;

        // Calculate initial required space (string only)
        int requiredSpace = alignedStringSize;

        // Check if there's enough space for the string
        if (currentOffset + requiredSpace > heapSize) {
            // Resize heap
            long newSize = fileAllocation.expandLocalHeapContents();
            byte[] newHeapData = new byte[(int) newSize];
            System.arraycopy(heapData, 0, newHeapData, 0, heapData.length); // Copy existing data
            localHeapContents = new HdfLocalHeapContents(newHeapData);
            this.heapContentsSize = HdfFixedPoint.of(newSize);
            this.heapContentsOffset = HdfFixedPoint.of(fileAllocation.getCurrentLocalHeapContentsOffset());
            heapData = newHeapData;
            heapSize = (int) newSize;
        }

        // Check if there's enough space for a free block after the string
        boolean includeFreeBlock = (heapSize - (currentOffset + alignedStringSize)) >= 16;
        if (includeFreeBlock) {
            requiredSpace += 16; // Add 16 bytes for free block
        }

        // Write string
        System.arraycopy(objectNameBytes, 0, heapData, currentOffset, objectNameBytes.length);
        heapData[currentOffset + objectNameBytes.length] = 0; // Null terminator
        Arrays.fill(heapData, currentOffset + stringSize, currentOffset + alignedStringSize, (byte) 0); // Padding

        // Update offset
        int newFreeListOffset = currentOffset + alignedStringSize;

        // Handle free list
        if (includeFreeBlock) {
            // Write free block metadata
            ByteBuffer buffer = ByteBuffer.wrap(heapData).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(newFreeListOffset, 1); // Next offset: 1 (last block)
            buffer.putLong(newFreeListOffset + 8, heapSize - newFreeListOffset); // Remaining space
            this.freeListOffset = HdfFixedPoint.of(newFreeListOffset);
        } else {
            // Set freeListOffset to actual offset unless heap is exactly full
            if (currentOffset + alignedStringSize == heapSize) {
                this.freeListOffset = HdfFixedPoint.of(1); // Heap full, mimic C++ behavior
            } else {
                this.freeListOffset = HdfFixedPoint.of(newFreeListOffset); // Use actual offset
            }
        }

        return new Pair<>(localHeapContents, currentOffset);
    }

    public static HdfLocalHeap readFromFileChannel(SeekableByteChannel fileChannel, short offsetSize, short lengthSize, HdfDataFile hdfDataFile) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

        fileChannel.read(buffer);
        buffer.flip();

        byte[] signatureBytes = new byte[4];
        buffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"HEAP".equals(signature)) {
            throw new IllegalArgumentException("Invalid heap signature: " + signature);
        }

        int version = Byte.toUnsignedInt(buffer.get());

        byte[] reserved = new byte[3];
        buffer.get(reserved);
        if (!allBytesZero(reserved)) {
            throw new IllegalArgumentException("Reserved bytes in heap header must be zero.");
        }

        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint dataSegmentSize = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short)(lengthSize*8));
        HdfFixedPoint freeListOffset = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint dataSegmentAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short) 0, (short)(offsetSize*8));

        return new HdfLocalHeap(signature, version, dataSegmentSize, freeListOffset, dataSegmentAddress, hdfDataFile);
    }

    private static boolean allBytesZero(byte[] bytes) {
        for (byte b : bytes) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "HdfLocalHeap{" +
                "signature='" + signature + '\'' +
                ", version=" + version +
                ", dataSegmentSize=" + heapContentsSize +
                ", freeListOffset=" + freeListOffset +
                ", dataSegmentAddress=" + heapContentsOffset +
                '}';
    }

    public void writeToByteChannel(SeekableByteChannel seekableByteChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        buffer.put(signature.getBytes());
        buffer.put((byte) version);
        buffer.put(new byte[3]);
        writeFixedPointToBuffer(buffer, heapContentsSize);
        writeFixedPointToBuffer(buffer, freeListOffset);
        writeFixedPointToBuffer(buffer, heapContentsOffset);

        buffer.rewind();
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }
    }
}