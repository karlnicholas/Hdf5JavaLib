package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.reference.HdfDataspaceSelectionInstance;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.hdffile.infrastructure.HdfBTreeEntryBase;
import org.hdf5javalib.hdffile.infrastructure.HdfBTreeV1;
import org.hdf5javalib.hdffile.infrastructure.HdfChunkBTreeEntry;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

import static org.hdf5javalib.hdfjava.HdfFileReader.BTREE_HEADER_INITIAL_SIZE;
import static org.hdf5javalib.hdfjava.HdfFileReader.BTREE_SIGNATURE;

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
        super(MessageType.DATA_LAYOUT_MESSAGE, sizeMessageData, flags);
        this.version = version;
        this.dataLayoutStorage = dataLayoutStorage;
    }

    private static FixedPointDatatype getFourByteUnsignedDatatype(HdfDataFile hdfDataFile) {
        return new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                4, 0, (4 * 8),
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
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
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
                dataLayoutStorage = new CompactStorage(compactDataSize, compactData, hdfDataFile);
                break;

            case 1: // Contiguous Storage
                HdfFixedPoint dataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                HdfFixedPoint dataSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), buffer);
                dataLayoutStorage = new ContiguousStorage(dataAddress, dataSize, hdfDataFile);
                break;

            case 2: // Chunked Storage
                FixedPointDatatype fourByteDt = getFourByteUnsignedDatatype(hdfDataFile);
                HdfFixedPoint chunkedDataAddress;
                HdfFixedPoint[] dimensionSizes;
                HdfFixedPoint datasetElementSize;
                HdfBTreeV1 bTree;
                int numDimensions;
                if (version == 1 || version == 2) {
                    chunkedDataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                    numDimensions = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                    dimensionSizes = new HdfFixedPoint[numDimensions];
                    for (int i = 0; i < numDimensions; i++) {
                        dimensionSizes[i] = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                    }
                    datasetElementSize = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                } else {
                    int dimensionality = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                    chunkedDataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                    numDimensions = dimensionality - 1;
                    dimensionSizes = new HdfFixedPoint[numDimensions];
                    for (int i = 0; i < numDimensions; i++) {
                        dimensionSizes[i] = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                    }
                    datasetElementSize = HdfReadUtils.readHdfFixedPointFromBuffer(fourByteDt, buffer);
                }
                try {
                    if (!chunkedDataAddress.isUndefined()) {
                        FixedPointDatatype eightByteFixedPointType =  new FixedPointDatatype(
                                FixedPointDatatype.createClassAndVersion(),
                                FixedPointDatatype.createClassBitField(false, false, false, false),
                                8, 0, (8 * 8),
                                hdfDataFile);

//                        bTree = HdfFileReader.readBTreeFromSeekableByteChannel(hdfDataFile.getSeekableByteChannel(), chunkedDataAddress.getInstance(Long.class), numDimensions, eightByteFixedPointType, hdfDataFile);
                        bTree = HdfFileReader.readBTreeFromSeekableByteChannel(hdfDataFile.getSeekableByteChannel(), chunkedDataAddress.getInstance(Long.class), hdfDataFile);
                    } else {
                        bTree = null;
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                dataLayoutStorage = new ChunkedStorage(chunkedDataAddress, dimensionSizes, datasetElementSize, bTree, hdfDataFile);
                break;

            case 3: // Virtual Storage
                if (version == 4) {
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
                }
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
        return dataLayoutStorage != null && dataLayoutStorage.hasData();
    }

    public DataLayoutStorage getDataLayoutStorage() throws IOException {
        return dataLayoutStorage;
    }

    public static abstract class DataLayoutStorage {
        protected final HdfDataFile hdfDataFile;
        public DataLayoutStorage(HdfDataFile hdfDataFile) {
            this.hdfDataFile = hdfDataFile;
        }
        abstract boolean hasData();

    }

    /**
     * Represents contiguous storage properties for a dataset.
     */
    public static class CompactStorage extends DataLayoutStorage {
        private final int compactDataSize;
        private final byte[] compactData;

        /**
         *
         * @param compactDataSize size of data
         * @param compactData      data
         */
        public CompactStorage(int compactDataSize, byte[] compactData, HdfDataFile hdfDataFile) {
            super(hdfDataFile);
            this.compactDataSize = compactDataSize;
            this.compactData = compactData;
        }

        public int getCompactDataSize() {
            return compactDataSize;
        }

        public byte[] getCompactData() {
            return compactData;
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

    }
    /**
     * Represents contiguous storage properties for a dataset.
     */
    public static class ContiguousStorage extends DataLayoutStorage {
        private final HdfFixedPoint dataAddress;
        private final HdfFixedPoint dataSize;

        /**
         * Constructs a ContiguousStorage instance.
         *
         * @param dataAddress the file address of the contiguous data
         * @param dataSize    the size of the contiguous data
         */
        public ContiguousStorage(HdfFixedPoint dataAddress, HdfFixedPoint dataSize, HdfDataFile hdfDataFile) {
            super(hdfDataFile);
            this.dataAddress = dataAddress;
            this.dataSize = dataSize;
        }

        public HdfFixedPoint getDataAddress() {
            return dataAddress;
        }

        public HdfFixedPoint getDataSize() {
            return dataSize;
        }

        /**
         * Returns a string representation of this ContiguousStorage.
         *
         * @return a string describing the version, address, and size
         */
        @Override
        public String toString() {
            return "ContiguousStorage{" +
                    ", address=" + (dataAddress.isUndefined()? HdfDisplayUtils.UNDEFINED :dataAddress) +
                    ", dataSize=" + (dataSize.isUndefined()?HdfDisplayUtils.UNDEFINED:dataSize) +
                    '}';
        }

        @Override
        public boolean hasData() {
            return HdfFixedPoint.compareToZero(dataSize) > 0
                    &&  !dataAddress.isUndefined();
        }

    }
    /**
     * Represents chunked storage properties for a dataset.
     */
    public static class ChunkedStorage extends DataLayoutStorage {
        private final HdfFixedPoint chunkedDataAddress;
        private final HdfFixedPoint[] dimensionSizes;
        private final HdfFixedPoint datasetElementSize;
        private final HdfBTreeV1 bTree;

        /**
         * Constructs a ChunkedStorage instance.
         *
         * @param dimensionSizes the sizes of each chunk dimension
         * @param chunkedDataAddress    the file address of the chunked data
         * @param datasetElementSize datasetElementSize
         */
        public ChunkedStorage(HdfFixedPoint chunkedDataAddress, HdfFixedPoint[] dimensionSizes, HdfFixedPoint datasetElementSize, HdfBTreeV1 bTree, HdfDataFile hdfDataFile) {
            super(hdfDataFile);
            this.chunkedDataAddress = chunkedDataAddress;
            this.dimensionSizes = dimensionSizes;
            this.datasetElementSize = datasetElementSize;
            this.bTree = bTree;
        }

        public HdfFixedPoint[] getDimensionSizes() {
            return dimensionSizes;
        }

        public HdfFixedPoint getDatasetElementSize() {
            return datasetElementSize;
        }

        public HdfBTreeV1 getBTree() {
            return bTree;
        }

        /**
         * Returns a string representation of this ChunkedStorage.
         *
         * @return a string describing the version, rank, chunk sizes, and address
         */
        @Override
        public String toString() {
            return "ChunkedStorage{" +
                    ", chunkedDataAddress=" + (chunkedDataAddress.isUndefined()? HdfDisplayUtils.UNDEFINED :chunkedDataAddress) +
                    ", dimensionSizes=" + Arrays.toString(dimensionSizes) +
                    ", datasetElementSize=" + datasetElementSize +
                    ", bTree=" + bTree +
                    '}';
        }

        @Override
        public boolean hasData() {
            return HdfFixedPoint.compareToZero(datasetElementSize) > 0
                    && dimensionSizes.length > 0
                    && HdfFixedPoint.compareToZero(dimensionSizes[0]) > 0
                    && bTree != null;
        }

    }

    /**
     * Writes the Data Layout message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
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
