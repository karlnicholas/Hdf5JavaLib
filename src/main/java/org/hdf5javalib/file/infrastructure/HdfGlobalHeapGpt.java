package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class HdfGlobalHeapGpt {
    private static final String SIGNATURE = "GCOL"; // HDF5 2.0 Global Heap Signature
    private static final int HEADER_SIZE = 24; // Increased from 16 to 24 bytes in HDF5 2.0

    private final int version;
    private final HdfFixedPoint dataSegmentSize;
    private HdfFixedPoint freeListOffset;
    private final HdfFixedPoint dataSegmentAddress;

    public HdfGlobalHeapGpt(int version, HdfFixedPoint dataSegmentSize, HdfFixedPoint freeListOffset, HdfFixedPoint dataSegmentAddress) {
        this.version = version;
        this.dataSegmentSize = dataSegmentSize;
        this.freeListOffset = freeListOffset;
        this.dataSegmentAddress = dataSegmentAddress;
    }

    public HdfGlobalHeapGpt(HdfFixedPoint dataSegmentSize, HdfFixedPoint dataSegmentAddress) {
        this(2, dataSegmentSize, HdfFixedPoint.of(0), dataSegmentAddress);
    }

    /**
     * Adds an object to the Global Heap (HDF5 2.0).
     */
    public int addToHeap(HdfString objectName, HdfLocalHeapContents localHeapContents) {
        byte[] objectNameBytes = objectName.getBytes();
        int freeListOffsetValue = this.freeListOffset.getInstance(Long.class).intValue();
        byte[] heapData = localHeapContents.getHeapData();

        // Read free space size from the current free list offset
        int freeBlockSize = ByteBuffer.wrap(heapData, freeListOffsetValue + 8, 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();

        // Ensure alignment to 16-byte boundary
        int requiredSize = (objectNameBytes.length + 15) & ~15;

        // Check if there is enough space for the new object
        if (requiredSize > freeBlockSize) {
            throw new IllegalStateException("Not enough space in the heap to add the object.");
        }

        // Copy object name into the heap at the free list offset
        System.arraycopy(objectNameBytes, 0, heapData, freeListOffsetValue, objectNameBytes.length);

        // Align free list offset to the next 16-byte boundary
        int newFreeListOffset = freeListOffsetValue + requiredSize;
        Arrays.fill(heapData, freeListOffsetValue + objectNameBytes.length, newFreeListOffset, (byte) 0);

        // Update remaining free space
        int remainingFreeSpace = freeBlockSize - requiredSize;
        this.freeListOffset = HdfFixedPoint.of(newFreeListOffset);

        // Write new free block metadata
        ByteBuffer buffer = ByteBuffer.wrap(heapData).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(newFreeListOffset, 1);  // Mark as last block
        buffer.putLong(newFreeListOffset + 8, remainingFreeSpace);  // Updated free space size
        return freeListOffsetValue;
    }

    /**
     * Reads a Global Heap from an HDF5 2.0 file.
     */
    public static HdfGlobalHeapGpt readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();

        // Parse signature
        byte[] signatureBytes = new byte[4];
        buffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"GCOL".equals(signature)) {
            throw new IllegalArgumentException("Invalid global heap signature: " + signature);
        }

        // Parse version
        int version = Byte.toUnsignedInt(buffer.get());

        // Parse reserved bytes (HDF5 2.0 uses 4 reserved bytes)
        byte[] reserved = new byte[4];
        buffer.get(reserved);
        if (!allBytesZero(reserved)) {
            throw new IllegalArgumentException("Reserved bytes in heap header must be zero.");
        }

        // Parse fixed-point fields
        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint dataSegmentSize = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short) (lengthSize * 8));
        HdfFixedPoint freeListOffset = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short) (offsetSize * 8));
        HdfFixedPoint dataSegmentAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short) 0, (short) (offsetSize * 8));

        return new HdfGlobalHeapGpt(version, dataSegmentSize, freeListOffset, dataSegmentAddress);
    }

    /**
     * Writes the Global Heap to a ByteBuffer.
     */
    public void writeToByteBuffer(ByteBuffer buffer) {
        buffer.put(SIGNATURE.getBytes());
        buffer.put((byte) version);
        buffer.put(new byte[4]); // Reserved bytes for HDF5 2.0

        writeFixedPointToBuffer(buffer, dataSegmentSize);
        writeFixedPointToBuffer(buffer, freeListOffset);
        writeFixedPointToBuffer(buffer, dataSegmentAddress);
    }

    /**
     * Helper function to check if all bytes in an array are zero.
     */
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
        return "HdfGlobalHeapGpt{" +
                "signature='" + SIGNATURE + '\'' +
                ", version=" + version +
                ", dataSegmentSize=" + dataSegmentSize +
                ", freeListOffset=" + freeListOffset +
                ", dataSegmentAddress=" + dataSegmentAddress +
                '}';
    }
}
