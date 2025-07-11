package org.hdf5javalib.maydo.hdffile.dataobjects.messages;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.dataclass.reference.HdfDataspaceSelectionInstance;
import org.hdf5javalib.maydo.datatype.FixedPointDatatype;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

/**
 * Represents a Data Layout Message in the HDF5 file format.
 * <p>
 * The {@code DataLayoutMessage} class defines how raw data is stored within an HDF5 file.
 * It specifies the data storage method (contiguous, chunked, or compact) and includes
 * details about data organization, offsets, and dimensions.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the data layout message format.</li>
 *   <li><b>Layout Class (1 byte)</b>: The storage method:
 *       <ul>
 *         <li><b>Contiguous (0)</b>: Data stored in a single continuous block.</li>
 *         <li><b>Chunked (1)</b>: Data divided into fixed-size chunks, allowing
 *             partial I/O and compression.</li>
 *         <li><b>Compact (2)</b>: Small datasets stored directly within the
 *             object header.</li>
 *       </ul>
 *   </li>
 *   <li><b>Storage Properties</b>: Varies by layout class:
 *       <ul>
 *         <li><b>Contiguous:</b> 8-byte file address for data start.</li>
 *         <li><b>Chunked:</b> Dimensionality, chunk dimensions, and chunk addresses.</li>
 *         <li><b>Compact:</b> Raw data stored in the message.</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h2>Layout Classes</h2>
 * <ul>
 *   <li><b>Contiguous</b>: Efficient for sequential access but not ideal for partial modifications.</li>
 *   <li><b>Chunked</b>: Supports compression, partial I/O, and expansion for large datasets.</li>
 *   <li><b>Compact</b>: Optimized for small datasets, reducing file overhead.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class DataLayoutMessage extends HdfMessage {
    /**
     * The version of the data layout message format.
     */
    private final int version;
    private final DataLayoutStorage dataLayoutStorage;

    /**
     * Constructs a DataLayoutMessage with the specified components.
     *
     * @param version            the version of the data layout message format
     * @param dataLayoutStorage  dataLayoutStorage
     * @param flags              message flags
     * @param sizeMessageData    the size of the message data in bytes
     */
    public DataLayoutMessage(
            int version,
            DataLayoutStorage dataLayoutStorage,
            int flags,
            int sizeMessageData
    ) {
        super(MessageType.DataLayoutMessage, sizeMessageData, flags);
        this.version = version;
        this.dataLayoutStorage = dataLayoutStorage;
    }

    private static FixedPointDatatype getFourByteUnsignedDatatype(HdfDataFile hdfDataFile) {
        return new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                4, (short) 0, (short) (4 * 8),
                hdfDataFile);
    }

    /**
     * Parses a DataLayoutMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for datatype and heap resources
     * @return a new DataLayoutMessage instance parsed from the data
     * @throws IllegalArgumentException if the version or layout class is unsupported
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Read version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());

        // Read layout class (1 byte)
        int layoutClass = Byte.toUnsignedInt(buffer.get());

        DataLayoutStorage dataLayoutStorage;
        // Parse based on layout class
        switch (layoutClass) {
            case 0: // Compact Storage
                int compactDataSize = Short.toUnsignedInt(buffer.getShort()); // Compact data size (2 bytes)
                byte[] compactData = compactData = new byte[compactDataSize];
                buffer.get(compactData); // Read compact data
                dataLayoutStorage = new CompactStorage(compactDataSize, compactData);
                break;

            case 1: // Contiguous Storage
                HdfFixedPoint dataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                HdfFixedPoint dataSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), buffer);
                dataLayoutStorage = new ContiguousStorage(dataAddress, dataSize);
                break;

            case 2: // Chunked Storage
                FixedPointDatatype fourByteDt = getFourByteUnsignedDatatype(hdfDataFile);
                HdfFixedPoint chunkedDataAddress;
                HdfFixedPoint[] dimensionSizes;
                HdfFixedPoint datasetElementSize;
                if (version == 1 || version == 2) {
                    chunkedDataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                    int numDimensions = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                    dimensionSizes = new HdfFixedPoint[numDimensions];
                    for (int i = 0; i < numDimensions; i++) {
                        dimensionSizes[i] = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                    }
                    datasetElementSize = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                } else {
                    int dimensionality = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                    chunkedDataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                    int numDimensions = dimensionality - 1;
                    dimensionSizes = new HdfFixedPoint[numDimensions];
                    for (int i = 0; i < numDimensions; i++) {
                        dimensionSizes[i] = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                    }
                    datasetElementSize = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                }
                dataLayoutStorage = new ChunkedStorage(chunkedDataAddress, dimensionSizes, datasetElementSize);
                break;

            case 3: // Virtual Storage
                if (version != 4) {
                    throw new IllegalArgumentException("Layout class 3 is only supported in version 4");
                }
                dataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                long index = Integer.toUnsignedLong(buffer.getInt());
                byte[] dataBytes = hdfDataFile.getGlobalHeap().getDataBytes(dataAddress, (int) index);
                ByteBuffer dataBuffer = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN);
                int vVersion = Byte.toUnsignedInt(dataBuffer.get());
                HdfFixedPoint numEntries = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), dataBuffer);
                int iNumEntries = numEntries.getInstance(Long.class).intValue();
                for (int i = 0; i < iNumEntries; i++) {
                    String sourceFilename = HdfReadUtils.readNullTerminatedString(dataBuffer);
                    String sourceDataset = HdfReadUtils.readNullTerminatedString(dataBuffer);
                    HdfDataspaceSelectionInstance sourceSelection = HdfDataspaceSelectionInstance.parseSelectionInfo(dataBuffer);
                    HdfDataspaceSelectionInstance virtualSelection = HdfDataspaceSelectionInstance.parseSelectionInfo(dataBuffer);
                }
                long checkSum = Integer.toUnsignedLong(dataBuffer.getInt());
                dataLayoutStorage = null;
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }

        // Return a constructed instance of DataLayoutMessage
        return new DataLayoutMessage(version, dataLayoutStorage, flags, data.length);
    }

    /**
     * Returns a string representation of this DataLayoutMessage.
     *
     * @return a string describing the message size, version, layout class, and storage properties
     */
    @Override
    public String toString() {
        return "DataLayoutMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                "dataLayoutStorage=" + dataLayoutStorage +
                '}';
    }

    public boolean hasData() {
        return dataLayoutStorage.hasData();
    }

    public ByteBuffer getData(SeekableByteChannel channel, long offset, long size) throws IOException {
        return dataLayoutStorage.getData(channel, offset, size);
    }

//    public void setDataAddress(HdfFixedPoint dataAddress) {
//        this.dataAddress = dataAddress;
//    }
//
//    public HdfFixedPoint getDataAddress() {
//        return dataLayoutStorage.getDataAddress();
//    }
//
//    public HdfFixedPoint[] getDimensionSizes() {
//        return d;
//    }

    public interface DataLayoutStorage {

        boolean hasData();

        ByteBuffer getData(SeekableByteChannel channel, long offset, long size) throws IOException ;
    }
    /**
     * Represents contiguous storage properties for a dataset.
     */
    public static class CompactStorage implements DataLayoutStorage {
        private final int compactDataSize;
        private final byte[] compactData;

        /**
         *
         * @param compactDataSize size of data
         * @param compactData      data
         */
        public CompactStorage(int compactDataSize, byte[] compactData) {
            this.compactDataSize = compactDataSize;
            this.compactData = compactData;
        }

        /**
         * Returns a string representation of this ContiguousStorage.
         *
         * @return a string describing the version, address, and size
         */
        @Override
        public String toString() {
            return "CompactStorage{" +
                    ", compactDataSize=" + compactDataSize +
                    ", compactData=" + compactData +
                    '}';
        }

        @Override
        public boolean hasData() {
            return compactDataSize > 0;
        }

        @Override
        public ByteBuffer getData(SeekableByteChannel channel, long offset, long size) {
            return null;
        }
    }
    /**
     * Represents contiguous storage properties for a dataset.
     */
    public static class ContiguousStorage implements DataLayoutStorage {
        private final HdfFixedPoint dataAddress;
        private final HdfFixedPoint dataSize;

        /**
         * Constructs a ContiguousStorage instance.
         *
         * @param dataAddress the file address of the contiguous data
         * @param dataSize    the size of the contiguous data
         */
        public ContiguousStorage(HdfFixedPoint dataAddress, HdfFixedPoint dataSize) {
            this.dataAddress = dataAddress;
            this.dataSize = dataSize;
        }

        /**
         * Returns a string representation of this ContiguousStorage.
         *
         * @return a string describing the version, address, and size
         */
        @Override
        public String toString() {
            return "ContiguousStorage{" +
                    ", address=" + dataAddress +
                    ", elementSize=" + dataSize +
                    '}';
        }

        @Override
        public boolean hasData() {
            return HdfFixedPoint.compareToZero(dataSize) > 0;
        }

        @Override
        public ByteBuffer getData(SeekableByteChannel channel, long offset, long size) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
            channel.position(dataAddress.getInstance(Long.class) + offset);
            int bytesRead = channel.read(buffer);
            if (bytesRead != size) {
                throw new IOException("Failed to read the expected number of bytes: read " + bytesRead + ", expected " + size);
            }
            buffer.flip();
            return buffer;
        }
    }
    /**
     * Represents chunked storage properties for a dataset.
     */
    public static class ChunkedStorage implements DataLayoutStorage {
        private final HdfFixedPoint chunkedDataAddress;
        private final HdfFixedPoint[] dimensionSizes;
        private final HdfFixedPoint datasetElementSize;

        /**
         * Constructs a ChunkedStorage instance.
         *
         * @param dimensionSizes the sizes of each chunk dimension
         * @param chunkedDataAddress    the file address of the chunked data
         * @param datasetElementSize datasetElementSize
         */
        public ChunkedStorage(HdfFixedPoint chunkedDataAddress, HdfFixedPoint[] dimensionSizes, HdfFixedPoint datasetElementSize) {
            this.chunkedDataAddress = chunkedDataAddress;
            this.dimensionSizes = dimensionSizes;
            this.datasetElementSize = datasetElementSize;
        }

        /**
         * Returns a string representation of this ChunkedStorage.
         *
         * @return a string describing the version, rank, chunk sizes, and address
         */
        @Override
        public String toString() {
            return "ChunkedStorage{" +
                    ", chunkedDataAddress=" + chunkedDataAddress +
                    ", dimensionSizes=" + Arrays.toString(dimensionSizes) +
                    ", datasetElementSize=" + datasetElementSize +
                    '}';
        }

        @Override
        public boolean hasData() {
            return HdfFixedPoint.compareToZero(datasetElementSize) > 0 && dimensionSizes.length > 0 && HdfFixedPoint.compareToZero(dimensionSizes[0]) > 0;
        }

        @Override
        public ByteBuffer getData(SeekableByteChannel channel, long offset, long size) throws IOException {
            return null;
        }
    }

    /**
     * Writes the Data Layout message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
//        writeMessageData(buffer);
//        // Write version (1 byte)
//        buffer.put((byte) version);
//        // Write layout class (1 byte)
//        buffer.put((byte) layoutClass);
//
//        switch (layoutClass) {
//            case 0: // Compact Storage
//                buffer.putShort((short) compactDataSize); // Compact data size (2 bytes)
//                buffer.put(compactData); // Write compact data
//                break;
//
//            case 1: // Contiguous Storage
//                writeFixedPointToBuffer(buffer, dataAddress);
//                writeFixedPointToBuffer(buffer, dimensionSizes[0]);
//                break;
//
//            case 2: // Chunked Storage
//                writeFixedPointToBuffer(buffer, dataAddress);
//                buffer.put((byte) dimensionSizes.length); // Number of dimensions (1 byte)
//                for (HdfFixedPoint dimensionSize : dimensionSizes) {
//                    writeFixedPointToBuffer(buffer, dimensionSize);
//                }
//                writeFixedPointToBuffer(buffer, datasetElementSize);
//                break;
//
//            default:
//                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
//        }
    }

}
