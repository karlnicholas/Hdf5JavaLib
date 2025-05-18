package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.AllocationRecord;
import org.hdf5javalib.redo.AllocationType;
import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.HdfFileAllocation;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.dataclass.HdfString;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents an HDF5 Local Heap as defined in the HDF5 specification.
 * <p>
 * The {@code HdfLocalHeap} class manages a local heap, which stores variable-length data
 * such as strings for a specific group in an HDF5 file. It supports reading from a file
 * channel, writing to a file channel, adding strings to the heap, and parsing strings
 * from the heap data.
 * </p>
 *
 * @see HdfDataFile
 * @see HdfFixedPoint
 * @see HdfString
 * @see HdfFileAllocation
 */
public class HdfLocalHeap extends AllocationRecord {
    /** The signature of the local heap ("HEAP"). */
    private final String signature;
    /** The version of the local heap format. */
    private final int version;
    /** The HDF5 file context. */
    private final HdfDataFile hdfDataFile;
    /** The raw byte array containing the heap's data. */
    private HdfLocalHeapData heapData;

    public HdfFixedPoint getFreeListOffset() {
        return heapData.getFreeListOffset();
    }

    /**
     * A utility class to hold a pair of values.
     *
     * @param <T> the type of the first value
     * @param <U> the type of the second value
     */
    public static class Pair<T, U> {
        private final T first;
        private final U second;

        /**
         * Constructs a Pair with the specified values.
         *
         * @param first  the first value
         * @param second the second value
         */
        public Pair(T first, U second) {
            this.first = first;
            this.second = second;
        }

        /**
         * Returns the first value of the pair.
         *
         * @return the first value
         */
        public T getFirst() {
            return first;
        }

        /**
         * Returns the second value of the pair.
         *
         * @return the second value
         */
        public U getSecond() {
            return second;
        }
    }

    /**
     * Constructs an {@code HdfLocalHeap} for reading an HDF5 local heap from a file.
     * Initializes the local heap with the provided signature, version, and heap properties,
     * associating it with the given HDF5 data file and heap data buffer.
     *
     * @param signature          the signature of the local heap (e.g., "HEAP")
     * @param version            the version of the local heap format
     * @param heapContentsSize   the size of the heap's data segment
     * @param heapContentsOffset the offset to the heap's data segment in the file
     * @param hdfDataFile        the HDF5 data file containing the local heap
     * @param heapData           the raw byte array containing the heap's data
     */
    public HdfLocalHeap(String signature, int version, HdfFixedPoint heapContentsSize,
                        HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile, HdfLocalHeapData heapData,
                        String name) {
        super(AllocationType.LOCAL_HEAP_HEADER, name, heapContentsOffset, heapContentsSize);
        this.signature = signature;
        this.version = version;
//        this.heapContentsSize = heapContentsSize;
//        this.freeListOffset = freeListOffset;
//        this.heapContentsOffset = heapContentsOffset;
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
     * @param heapContentsSize   the size of the heap's data segment
     * @param heapContentsOffset the offset to the heap's data segment in the file
     * @param hdfDataFile        the HDF5 data file to which the local heap will be written
     */
    public HdfLocalHeap(HdfFixedPoint heapContentsSize, HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile, String name) {
        super(AllocationType.LOCAL_HEAP_HEADER, name, heapContentsOffset, heapContentsSize);
        this.signature = "HEAP";
        this.version = 0;
//        this.heapContentsSize = heapContentsSize;
//        this.freeListOffset = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset());
//        this.heapContentsOffset = heapContentsOffset;
        this.hdfDataFile = hdfDataFile;
//        long localHeapContentsSize = hdfDataFile.getFileAllocation().getCurrentLocalHeapContentsSize();
        heapData = new HdfLocalHeapData(heapContentsOffset, heapContentsSize, hdfDataFile);
        addToHeap("");
//        heapData[0] = (byte)0x1;
//        heapData[8] = (byte)localHeapContentsSize;

    }

    /**
     * Adds a string to the local heap and returns its offset.
     *
     * @param objectName the string to add to the heap
     * @return the offset in the heap where the string is stored
     */
//    public int addToHeap(HdfString objectName) {
    public HdfFixedPoint addToHeap(String objectName) {
        return heapData.addToHeap(objectName, hdfDataFile);
    }

    /**
     * Reads an HdfLocalHeap from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfLocalHeap
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the heap signature or reserved bytes are invalid
     */
    public static HdfLocalHeap readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile
    ) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

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

//        HdfLocalHeapData hdfLocalHeapData = HdfLocalHeapData.readFromSeekableByteChannel(fileChannel, hdfDataFile);
        HdfFixedPoint dataSegmentSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint freeListOffset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint dataSegmentAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);

        HdfLocalHeapData hdfLocalHeapData = HdfLocalHeapData.readFromSeekableByteChannel(
                fileChannel, dataSegmentSize, freeListOffset, dataSegmentAddress);

        //
//        fileChannel.position(dataSegmentAddress.getInstance(Long.class));
//        // Allocate buffer and read heap data from the file channel
//        byte[] heapData = new byte[dataSegmentSize.getInstance(Long.class).intValue()];
//        buffer = ByteBuffer.wrap(heapData);
//
//        fileChannel.read(buffer);

//        String signature, int version, HdfFixedPoint heapContentsSize,
//                HdfFixedPoint freeListOffset, HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile, HdfLocalHeapData heapData,
//                String name
//TODO:
//        // wrong hdfLocalHeapData, needs to be prior one
//        String objectName = hdfLocalHeapData.getStringAtOffset(linkNameOffset);
        return new HdfLocalHeap(signature, version,
                dataSegmentSize,
                dataSegmentAddress,
                hdfDataFile, hdfLocalHeapData,
                ":local heap");
    }

    /**
     * Checks if all bytes in an array are zero.
     *
     * @param bytes the byte array to check
     * @return true if all bytes are zero, false otherwise
     */
    private static boolean allBytesZero(byte[] bytes) {
        for (byte b : bytes) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a string representation of the HdfLocalHeap.
     *
     * @return a string describing the heap's signature, version, sizes, offsets, and data
     */
    @Override
    public String toString() {
        return "HdfLocalHeap{" +
                "signature='" + signature + '\'' +
                ", version=" + version +
//                ", dataSegmentSize=" + heapContentsSize +
//                ", freeListOffset=" + freeListOffset +
//                ", dataSegmentAddress=" + heapContentsOffset +
//                ", heapDataSize=" + heapData.length +
                ", heapData=" + heapData +'}';
    }

    /**
     * Writes the local heap to a file channel.
     *
     * @param seekableByteChannel the file channel to write to
     * @param fileAllocation      the file allocation manager
     * @throws IOException if an I/O error occurs
     */
    public void writeToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);

        buffer.put(signature.getBytes());
        buffer.put((byte) version);
        buffer.put(new byte[3]);

        writeFixedPointToBuffer(buffer, heapData.getHeapContentsSize());
        writeFixedPointToBuffer(buffer, heapData.getFreeListOffset());
        writeFixedPointToBuffer(buffer, heapData.getHeapContentsOffset());

        buffer.rewind();
        seekableByteChannel.position(getOffset().getInstance(Long.class));
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }

        heapData.writeToByteChannel(seekableByteChannel, hdfDataFile);
//
//        buffer = ByteBuffer.wrap(heapData);
//        seekableByteChannel.position(hdfDataFile.getFileAllocation().getCurrentLocalHeapContentsOffset());
//        while (buffer.hasRemaining()) {
//            seekableByteChannel.write(buffer);
//        }
    }

    /**
     * Parses the next null-terminated string from the heap data at the specified offset.
     *
     * @param offset the offset in the heap data to start parsing
     * @return the parsed HdfString, or null if no valid string is found
     */
    public String parseStringAtOffset(HdfFixedPoint offset) {
//        long iOffset = offset.getInstance(Long.class);
//        if (iOffset >= heapData.length) {
//            return null; // End of heap data
//        }
//
//        long start = iOffset;
//
//        // Find the null terminator
//        while (iOffset < heapData.length && heapData[(int) iOffset] != 0) {
//            iOffset++;
//        }
//        ;
        // Extract the string
        return heapData.getStringAtOffset(offset);
//        return new HdfString(value,
//            new StringDatatype(
//                    StringDatatype.createClassAndVersion(),
//                    StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII),
//                    value.length())
//        );
    }
}