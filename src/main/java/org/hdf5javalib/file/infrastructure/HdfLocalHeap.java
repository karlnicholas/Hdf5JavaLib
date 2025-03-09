package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;

import java.io.IOException;
import java.math.BigInteger;
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
    private final HdfFixedPoint dataSegmentSize;
    private HdfFixedPoint freeListOffset;
    private final HdfFixedPoint dataSegmentAddress;

    public HdfLocalHeap(String signature, int version, HdfFixedPoint dataSegmentSize, HdfFixedPoint freeListOffset, HdfFixedPoint dataSegmentAddress) {
        this.signature = signature;
        this.version = version;
        this.dataSegmentSize = dataSegmentSize;
        this.freeListOffset = freeListOffset;
        this.dataSegmentAddress = dataSegmentAddress;
    }

    public HdfLocalHeap(HdfFixedPoint dataSegmentSize, HdfFixedPoint dataSegmentAddress) {
        this("HEAP", 0, dataSegmentSize, HdfFixedPoint.of(0), dataSegmentAddress);
    }

    public int addToHeap(HdfString objectName, HdfLocalHeapContents localHeapContents) {
        byte[] objectNameBytes = objectName.getBytes();
        int freeListOffset = this.freeListOffset.getInstance(Integer.class);
        byte[] heapData = localHeapContents.getHeapData();

        // ✅ Extract free space size from the current freeListOffset location
        int freeBlockSize = ByteBuffer.wrap(heapData, freeListOffset + 8, 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();

        // ✅ Check if there is enough space for the new objectName
        if (objectNameBytes.length > freeBlockSize) {
            throw new IllegalStateException("Not enough space in the heap to add the objectName.");
        }

        // ✅ Copy the new objectName into the heap at the freeListOffset
        System.arraycopy(objectNameBytes, 0, heapData, freeListOffset, objectNameBytes.length);

        // ✅ Align freeListOffset to the next 8-byte boundary
        int newFreeListOffset;
        if (objectNameBytes.length == 0 ) {
            newFreeListOffset = freeListOffset + 8;
        } else {
            newFreeListOffset = (freeListOffset + objectNameBytes.length + 7) & ~7;
        }
        Arrays.fill(heapData, freeListOffset + objectNameBytes.length, newFreeListOffset, (byte) 0);

        // ✅ Calculate the remaining free space
        int remainingFreeSpace = freeBlockSize - (newFreeListOffset - freeListOffset);

        // ✅ Store updated freeListOffset
        this.freeListOffset = HdfFixedPoint.of(newFreeListOffset);

        // ✅ Write new free block metadata at the **new** freeListOffset
        ByteBuffer buffer = ByteBuffer.wrap(heapData).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(newFreeListOffset, 1);  // Mark as last block
        buffer.putLong(newFreeListOffset + 8, remainingFreeSpace);  // Updated free space size
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
                ", dataSegmentSize=" + dataSegmentSize +
                ", freeListOffset=" + freeListOffset +
                ", dataSegmentAddress=" + dataSegmentAddress +
                '}';
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
        // Step 1: Write the "HEAP" signature (4 bytes)
        buffer.put(signature.getBytes());

        // Step 2: Write the version (1 byte)
        buffer.put((byte) version);

        // Step 3: Write reserved bytes (3 bytes, must be 0)
        buffer.put(new byte[3]);

        // Step 4: Write Data Segment Size (lengthSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, dataSegmentSize);

        // Step 5: Write Free List Offset (lengthSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, freeListOffset);

        // Step 6: Write Data Segment Address (offsetSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, dataSegmentAddress);
    }
}
