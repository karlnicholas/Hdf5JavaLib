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
        if (heapData == null || freeListOffset < 0 || freeListOffset > heapData.length) {
            throw new IllegalStateException("Invalid heap state: freeListOffset=" + freeListOffset + ", heapData.length=" + (heapData == null ? 0 : heapData.length));
        }

        // Calculate required space
        int requiredSpace = objectNameBytes.length + ((objectNameBytes.length == 0) ? 8 :
                ((objectNameBytes.length + 7) & ~7) - objectNameBytes.length + 16);

        // Check available space
        int availableSpace = heapData.length - freeListOffset;
        int freeBlockSize = 0;
        if (freeListOffset + 16 <= heapData.length) {
            freeBlockSize = ByteBuffer.wrap(heapData, freeListOffset + 8, 8)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .getInt();
        }

        // Expand heap if needed
        if (requiredSpace > availableSpace) {
            byte[] oldHeapData = heapData;
            int oldHeapLength = oldHeapData.length;
            long newSize = fileAllocation.expandLocalHeapContents();
            if (newSize <= oldHeapLength) {
                throw new IllegalStateException("Heap expansion failed: newSize=" + newSize + " <= oldHeapLength=" + oldHeapLength);
            }
            localHeapContents = new HdfLocalHeapContents(new byte[(int) newSize]);
            this.heapContentsSize = HdfFixedPoint.of(newSize);
            this.heapContentsOffset = HdfFixedPoint.of(fileAllocation.getCurrentLocalHeapContentsOffset());
            heapData = localHeapContents.getHeapData();
            System.arraycopy(oldHeapData, 0, heapData, 0, oldHeapLength);
            freeListOffset = oldHeapLength;
            this.freeListOffset = HdfFixedPoint.of(freeListOffset);
            availableSpace = heapData.length - freeListOffset;
            freeBlockSize = availableSpace - 16;
            // Initialize free block metadata
            ByteBuffer buffer = ByteBuffer.wrap(heapData).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(freeListOffset, 1);
            buffer.putLong(freeListOffset + 8, freeBlockSize);
        } else if (requiredSpace > freeBlockSize) {
            throw new IllegalStateException("Insufficient free block size: requiredSpace=" + requiredSpace + ", freeBlockSize=" + freeBlockSize);
        }

        // Store linkNameOffset for return
        int linkNameOffset = freeListOffset;

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

        return new Pair<>(localHeapContents, linkNameOffset);
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