package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.hdffile.AllocationRecord;
import org.hdf5javalib.redo.hdffile.AllocationType;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.HdfFileAllocation;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.dataclass.HdfString;
import org.hdf5javalib.redo.utils.HdfReadUtils;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

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
    private static final byte[] LOCAL_HEAP_SIGNATURE = new byte[]{'H', 'E', 'A', 'P'};
    private static final int LOCAL_HEAP_HEADER_SIZE=32;
    private static final int LOCAL_HEAP_HEADER_RESERVED_SIZE=3;
    /**
     * The version of the local heap format.
     */
    private final int version;
    /**
     * The HDF5 file context.
     */
    private final HdfDataFile hdfDataFile;
    /**
     * The raw byte array containing the heap's data.
     */
    private final HdfLocalHeapData heapData;

    public HdfFixedPoint getFreeListOffset() {
        return heapData.getFreeListOffset();
    }

    /**
     * Constructs an {@code HdfLocalHeap} for reading an HDF5 local heap from a file.
     * Initializes the local heap with the provided signature, version, and heap properties,
     * associating it with the given HDF5 data file and heap data buffer.
     *
     * @param version            the version of the local heap format
     * @param hdfDataFile        the HDF5 data file containing the local heap
     * @param heapData           the raw byte array containing the heap's data
     * @param heapOffset
     */
    public HdfLocalHeap(int version, HdfDataFile hdfDataFile, HdfLocalHeapData heapData,
                        String name, long heapOffset) {
        super(AllocationType.LOCAL_HEAP_HEADER, name,
                HdfWriteUtils.hdfFixedPointFromValue(heapOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                hdfDataFile.getFileAllocation().HDF_LOCAL_HEAP_HEADER_SIZE,
                hdfDataFile.getFileAllocation());
        this.version = version;
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
    public HdfLocalHeap(HdfFixedPoint heapContentsSize, HdfFixedPoint heapContentsOffset, HdfDataFile hdfDataFile, String name, long heapOffset) {
        super(AllocationType.LOCAL_HEAP_HEADER, name,
                HdfWriteUtils.hdfFixedPointFromValue(heapOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                hdfDataFile.getFileAllocation().HDF_INITIAL_LOCAL_HEAP_CONTENTS_SIZE,
                hdfDataFile.getFileAllocation());
        this.version = 0;
        this.hdfDataFile = hdfDataFile;
        heapData = new HdfLocalHeapData(heapContentsOffset, heapContentsSize, hdfDataFile);
        addToHeap("");
    }

    /**
     * Adds a string to the local heap and returns its offset.
     *
     * @param objectName the string to add to the heap
     * @return the offset in the heap where the string is stored
     */
    public HdfFixedPoint addToHeap(String objectName) {
        return heapData.addToHeap(objectName, hdfDataFile);
    }

    /**
     * Reads an HdfLocalHeap from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfLocalHeap
     * @throws IOException              if an I/O error occurs
     * @throws IllegalArgumentException if the heap signature or reserved bytes are invalid
     */
    public static HdfLocalHeap readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            String objectName
    ) throws IOException {
        long heapOffset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(LOCAL_HEAP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);

        fileChannel.read(buffer);
        buffer.flip();

        byte[] signatureBytes = new byte[LOCAL_HEAP_SIGNATURE.length];
        buffer.get(signatureBytes);
//        String signature = new String(signatureBytes);
        if (Arrays.compare(LOCAL_HEAP_SIGNATURE, signatureBytes) != 0) {
            throw new IllegalArgumentException("Invalid heap signature: " + signatureBytes);
        }

        int version = Byte.toUnsignedInt(buffer.get());

        buffer.position(buffer.position() + LOCAL_HEAP_HEADER_RESERVED_SIZE);

        HdfFixedPoint dataSegmentSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength(), buffer);
        HdfFixedPoint freeListOffset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint dataSegmentAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);

        HdfLocalHeapData hdfLocalHeapData = HdfLocalHeapData.readFromSeekableByteChannel(
                fileChannel, dataSegmentSize, freeListOffset, dataSegmentAddress, hdfDataFile, objectName);

        return new HdfLocalHeap(version,
                hdfDataFile, hdfLocalHeapData,
                objectName + ":Local Heap Header", heapOffset);
    }

    /**
     * Returns a string representation of the HdfLocalHeap.
     *
     * @return a string describing the heap's signature, version, sizes, offsets, and data
     */
    @Override
    public String toString() {
        return "HdfLocalHeap{" +
                ", version=" + version +
                ", heapData=" + heapData + '}';
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

        buffer.put(LOCAL_HEAP_SIGNATURE);
        buffer.put((byte) version);
        buffer.put(new byte[LOCAL_HEAP_HEADER_RESERVED_SIZE]);

        writeFixedPointToBuffer(buffer, heapData.getHeapContentsSize());
        writeFixedPointToBuffer(buffer, heapData.getFreeListOffset());
        writeFixedPointToBuffer(buffer, heapData.getHeapContentsOffset());

        buffer.rewind();
        seekableByteChannel.position(getOffset().getInstance(Long.class));
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }

        heapData.writeToByteChannel(seekableByteChannel, hdfDataFile);
    }

    /**
     * Parses the next null-terminated string from the heap data at the specified offset.
     *
     * @param offset the offset in the heap data to start parsing
     * @return the parsed HdfString, or null if no valid string is found
     */
    public String stringAtOffset(HdfFixedPoint offset) {
        return heapData.getStringAtOffset(offset);
    }
}