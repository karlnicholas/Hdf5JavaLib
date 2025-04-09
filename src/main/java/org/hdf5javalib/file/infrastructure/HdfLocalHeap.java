package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfFileAllocation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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

    public HdfLocalHeap(String signature, int version, HdfFixedPoint heapContentsSize,
                        HdfFixedPoint freeListOffset, HdfFixedPoint heapContentsOffset) {
        this.signature = signature;
        this.version = version;
        this.heapContentsSize = heapContentsSize;
        this.freeListOffset = freeListOffset;
        this.heapContentsOffset = heapContentsOffset;
    }

    public HdfLocalHeap(HdfFixedPoint heapContentsSize, HdfFixedPoint heapContentsOffset) {
        this("HEAP", 0, heapContentsSize, HdfFixedPoint.of(0), heapContentsOffset);
    }

    public int addToHeap(HdfString objectName, HdfLocalHeapContents localHeapContents) {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        byte[] objectNameBytes = objectName.getBytes();
        int freeListOffset = this.freeListOffset.getInstance(Long.class).intValue();
        byte[] heapData = localHeapContents.getHeapData();

        // Extract free space size
        int freeBlockSize = ByteBuffer.wrap(heapData, freeListOffset + 8, 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();

        // Check if there’s enough space; if not, request resize from HdfFileAllocation
        int requiredSpace = objectNameBytes.length + ((objectNameBytes.length == 0) ? 8 :
                ((objectNameBytes.length + 7) & ~7) - objectNameBytes.length + 16);
        if (requiredSpace > freeBlockSize) {
            // Call resizeHeap, assuming HdfFileAllocation knows current size
            long newSize = fileAllocation.expandLocalHeapContents();
            localHeapContents = new HdfLocalHeapContents(new byte[(int) newSize]);
            this.heapContentsSize = HdfFixedPoint.of(newSize);
            this.heapContentsOffset = HdfFixedPoint.of(fileAllocation.getCurrentLocalHeapContentsOffset());
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
    public static HdfLocalHeap readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize) throws IOException {
        // Allocate buffer for the local heap header
        ByteBuffer buffer = ByteBuffer.allocate(32); // Initial size for header parsing
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

        fileChannel.read(buffer);
        buffer.flip();

        // Parse the signature
        byte[] signatureBytes = new byte[4];
        buffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"HEAP".equals(signature)) {
            throw new IllegalArgumentException("Invalid heap signature: " + signature);
        }

        // Parse the version
        int version = Byte.toUnsignedInt(buffer.get());

        // Parse reserved bytes
        byte[] reserved = new byte[3];
        buffer.get(reserved);
        if (!allBytesZero(reserved)) {
            throw new IllegalArgumentException("Reserved bytes in heap header must be zero.");
        }

        // Parse fixed-point fields using HdfFixedPoint
        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint dataSegmentSize = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short)(lengthSize*8));
        HdfFixedPoint freeListOffset = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint dataSegmentAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short) 0, (short)(offsetSize*8));

        return new HdfLocalHeap(signature, version, dataSegmentSize, freeListOffset, dataSegmentAddress);
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

    public void writeToByteBuffer(ByteBuffer buffer) {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        buffer.position((int)(fileAllocation.getLocalHeapOffset() - fileAllocation.getRootGroupOffset()));

        // Step 1: Write the "HEAP" signature (4 bytes)
        buffer.put(signature.getBytes());

        // Step 2: Write the version (1 byte)
        buffer.put((byte) version);

        // Step 3: Write reserved bytes (3 bytes, must be 0)
        buffer.put(new byte[3]);

        // Step 4: Write Data Segment Size (lengthSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, heapContentsSize);

        // Step 5: Write Free List Offset (lengthSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, freeListOffset);

        // Step 6: Write Data Segment Address (offsetSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, heapContentsOffset);
    }
}