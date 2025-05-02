package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.utils.HdfReadUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class HdfLocalHeap {
    private final String signature;
    private final int version;
    private HdfFixedPoint heapContentsSize;
    private HdfFixedPoint freeListOffset;
    private HdfFixedPoint heapContentsOffset;
    private final HdfDataFile hdfDataFile;
    private byte[] heapData;


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

    /**
     * Constructs an {@code HdfLocalHeap} for reading an HDF5 local heap from a file.
     * Initializes the local heap with the provided signature, version, and heap properties,
     * associating it with the given HDF5 data file and heap data buffer.
     *
     * @param signature the signature of the local heap (e.g., "HEAP")
     * @param version the version of the local heap format
     * @param heapContentsSize the size of the heap's data segment
     * @param freeListOffset the offset to the free list within the heap
     * @param heapContentsOffset the offset to the heap's data segment in the file
     * @param hdfDataFile the HDF5 data file containing the local heap
     * @param heapData the raw byte array containing the heap's data
     */
    public HdfLocalHeap(String signature, int version, HdfFixedPoint heapContentsSize,
                        HdfFixedPoint freeListOffset, HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile, byte[] heapData) {
        this.signature = signature;
        this.version = version;
        this.heapContentsSize = heapContentsSize;
        this.freeListOffset = freeListOffset;
        this.heapContentsOffset = heapContentsOffset;
        this.hdfDataFile = hdfDataFile;
        this.heapData = heapData;
    }

    /**
     * Constructs an {@code HdfLocalHeap} for writing a new HDF5 local heap to a file.
     * Initializes the local heap with a default signature ("HEAP") and version (0),
     * using the specified heap size and offset, and associates it with the given HDF5 data file.
     * Allocates a heap data buffer based on the file allocation's current local heap size,
     * initializing it with specific byte values.
     *
     * @param heapContentsSize the size of the heap's data segment
     * @param heapContentsOffset the offset to the heap's data segment in the file
     * @param hdfDataFile the HDF5 data file to which the local heap will be written
     */
    public HdfLocalHeap(HdfFixedPoint heapContentsSize, HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile) {
        this.signature = "HEAP";
        this.version = 0;
        this.heapContentsSize = heapContentsSize;
        this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFixedPointDatatypeForOffset());
        this.heapContentsOffset = heapContentsOffset;
        this.hdfDataFile = hdfDataFile;
        long localHeapContentsSize = hdfDataFile.getFileAllocation().getCurrentLocalHeapContentsSize();
        heapData = new byte[(int) localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;
    }

    public int addToHeap(HdfString objectName) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        byte[] objectNameBytes = objectName.getBytes();
        int freeListOffset = this.freeListOffset.getInstance(Long.class).intValue();
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
            this.heapContentsSize = HdfWriteUtils.hdfFixedPointFromValue(newSize, hdfDataFile.getFixedPointDatatypeForLength());
            this.heapContentsOffset = HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getCurrentLocalHeapContentsOffset(), hdfDataFile.getFixedPointDatatypeForLength());
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
            this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(newFreeListOffset, hdfDataFile.getFixedPointDatatypeForLength());
        } else {
            // Set freeListOffset to actual offset unless heap is exactly full
            if (currentOffset + alignedStringSize == heapSize) {
                this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(1, hdfDataFile.getFixedPointDatatypeForLength()); // Heap full, mimic C++ behavior
            } else {
                this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(newFreeListOffset, hdfDataFile.getFixedPointDatatypeForLength()); // Use actual offset
            }
        }

        return currentOffset;
    }

    public static HdfLocalHeap readFromFileChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws IOException {
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

        HdfFixedPoint dataSegmentSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint freeListOffset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint dataSegmentAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);

        fileChannel.position(dataSegmentAddress.getInstance(Long.class));
        // Allocate buffer and read heap data from the file channel
        byte[] heapData = new byte[dataSegmentSize.getInstance(Long.class).intValue()];
        buffer = ByteBuffer.wrap(heapData);

        fileChannel.read(buffer);

        return new HdfLocalHeap(signature, version, dataSegmentSize, freeListOffset, dataSegmentAddress, hdfDataFile, heapData);
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
                ", heapDataSize=" + heapData.length +
                ", heapData=" + Arrays.toString(Arrays.copyOf(heapData, Math.min(heapData.length, 32))) +
                "... (truncated)" +
                '}';
    }

    public void writeToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        buffer.put(signature.getBytes());
        buffer.put((byte) version);
        buffer.put(new byte[3]);
        writeFixedPointToBuffer(buffer, heapContentsSize);
        writeFixedPointToBuffer(buffer, freeListOffset);
        writeFixedPointToBuffer(buffer, heapContentsOffset);

        buffer.rewind();
        seekableByteChannel.position(fileAllocation.getLocalHeapHeaderRecord().getOffset());
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }

        buffer = ByteBuffer.wrap(heapData);
        seekableByteChannel.position(hdfDataFile.getFileAllocation().getCurrentLocalHeapContentsOffset());
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }
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

}