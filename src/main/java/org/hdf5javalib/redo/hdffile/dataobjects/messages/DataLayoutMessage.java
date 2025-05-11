package org.hdf5javalib.redo.hdffile.dataobjects.messages;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.datatype.FixedPointDatatype;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

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
    /** The version of the data layout message format. */
    private final int version;
    /** The layout class (0: Contiguous, 1: Chunked, 2: Compact). */
    private final int layoutClass;
    /** The file address where data begins (for Contiguous or Chunked). */
    private HdfFixedPoint dataAddress;
    /** The dimensions of the dataset or chunks (for Contiguous or Chunked). */
    private final HdfFixedPoint[] dimensionSizes;
    /** The size of the compact data (for Compact). */
    private final int compactDataSize;
    /** The raw data stored in the message (for Compact). */
    private final byte[] compactData;
    /** The size of each dataset element (for Chunked). */
    private final HdfFixedPoint datasetElementSize;

    /**
     * Constructs a DataLayoutMessage with the specified components.
     *
     * @param version             the version of the data layout message format
     * @param layoutClass         the layout class (0: Contiguous, 1: Chunked, 2: Compact)
     * @param dataAddress         the file address for data (Contiguous or Chunked)
     * @param dimensionSizes      the dimensions of the dataset or chunks (Contiguous or Chunked)
     * @param compactDataSize     the size of compact data (Compact)
     * @param compactData         the raw data (Compact)
     * @param datasetElementSize  the size of each dataset element (Chunked)
     * @param flags               message flags
     * @param sizeMessageData     the size of the message data in bytes
     */
    public DataLayoutMessage(
            int version,
            int layoutClass,
            HdfFixedPoint dataAddress,
            HdfFixedPoint[] dimensionSizes,
            int compactDataSize,
            byte[] compactData,
            HdfFixedPoint datasetElementSize,
            byte flags,
            short sizeMessageData
    ) {
        super(MessageType.DataLayoutMessage, sizeMessageData, flags);
        this.version = version;
        this.layoutClass = layoutClass;
        this.dataAddress = dataAddress;
        this.dimensionSizes = dimensionSizes;
        this.compactDataSize = compactDataSize;
        this.compactData = compactData;
        this.datasetElementSize = datasetElementSize;
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
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Read version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());
        if (version < 1 || version > 3) {
            throw new IllegalArgumentException("Unsupported Data Layout Message version: " + version);
        }

        // Read layout class (1 byte)
        int layoutClass = Byte.toUnsignedInt(buffer.get());

        // Initialize fields
        HdfFixedPoint dataAddress = null;
        HdfFixedPoint[] dimensionSizes = null;
        int compactDataSize = 0;
        byte[] compactData = null;
        HdfFixedPoint datasetElementSize = null;

        // Parse based on layout class
        switch (layoutClass) {
            case 0: // Compact Storage
                compactDataSize = Short.toUnsignedInt(buffer.getShort()); // Compact data size (2 bytes)
                compactData = new byte[compactDataSize];
                buffer.get(compactData); // Read compact data
                break;

            case 1: // Contiguous Storage
                dataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                dimensionSizes = new HdfFixedPoint[1];
                dimensionSizes[0] = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                break;

            case 2: // Chunked Storage
                dataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                int numDimensions = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                dimensionSizes = new HdfFixedPoint[numDimensions];
                for (int i = 0; i < numDimensions; i++) {
                    dimensionSizes[i] = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
                }
                // Dataset element size (4 bytes)
                FixedPointDatatype fourByteFixedPointDatatype = new FixedPointDatatype(
                        FixedPointDatatype.createClassAndVersion(),
                        FixedPointDatatype.createClassBitField(false, false, false, false),
                        4, (short) 0, (short) (4*8)
                );
                byte[] fourByteBytes = new byte[4];
                buffer.get(fourByteBytes);
                datasetElementSize = fourByteFixedPointDatatype.getInstance(HdfFixedPoint.class, fourByteBytes);
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }

        // Return a constructed instance of DataLayoutMessage
        return new DataLayoutMessage(version, layoutClass, dataAddress, dimensionSizes, compactDataSize, compactData, datasetElementSize, flags, (short)data.length);
    }

    /**
     * Returns a string representation of this DataLayoutMessage.
     *
     * @return a string describing the message size, version, layout class, and storage properties
     */
    @Override
    public String toString() {
        return "DataLayoutMessage("+(getSizeMessageData()+8)+"){" +
                "version=" + version +
                ", layoutClass=" + layoutClass +
                ", dataAddress=" + (layoutClass == 1 || layoutClass == 2 ? dataAddress : "N/A") +
                ", dimensionSizes=" + Arrays.toString(dimensionSizes) +
                ", compactDataSize=" + (layoutClass == 0 ? compactDataSize : "N/A") +
                ", compactData=" + (layoutClass == 0 ? Arrays.toString(compactData) : "N/A") +
                ", datasetElementSize=" + (layoutClass == 2 ? datasetElementSize : "N/A") +
                '}';
    }

    /**
     * Writes the Data Layout message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write version (1 byte)
        buffer.put((byte) version);
        // Write layout class (1 byte)
        buffer.put((byte) layoutClass);

        switch (layoutClass) {
            case 0: // Compact Storage
                buffer.putShort((short) compactDataSize); // Compact data size (2 bytes)
                buffer.put(compactData); // Write compact data
                break;

            case 1: // Contiguous Storage
                writeFixedPointToBuffer(buffer, dataAddress);
                writeFixedPointToBuffer(buffer, dimensionSizes[0]);
                break;

            case 2: // Chunked Storage
                writeFixedPointToBuffer(buffer, dataAddress);
                buffer.put((byte) dimensionSizes.length); // Number of dimensions (1 byte)
                for (HdfFixedPoint dimensionSize : dimensionSizes) {
                    writeFixedPointToBuffer(buffer, dimensionSize);
                }
                writeFixedPointToBuffer(buffer, datasetElementSize);
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }
    }

    public void setDataAddress(HdfFixedPoint dataAddress) {
        this.dataAddress = dataAddress;
    }

    public HdfFixedPoint getDataAddress() {
        return dataAddress;
    }

    public HdfFixedPoint[] getDimensionSizes() {
        return dimensionSizes;
    }

    /**
     * Represents chunked storage properties for a dataset.
     */
    public static class ChunkedStorage {
        private final int version;
        private final int rank;
        private final long[] chunkSizes;
        private final HdfFixedPoint address;

        /**
         * Constructs a ChunkedStorage instance.
         *
         * @param version     the version of the chunked storage message
         * @param rank        the number of dimensions (rank)
         * @param chunkSizes  the sizes of each chunk dimension
         * @param address     the file address of the chunked data
         */
        public ChunkedStorage(int version, int rank, long[] chunkSizes, HdfFixedPoint address) {
            this.version = version;
            this.rank = rank;
            this.chunkSizes = chunkSizes;
            this.address = address;
        }

        /**
         * Parses chunked storage properties from the provided data.
         *
         * @param flags      message flags
         * @param data       the byte array containing the message data
         * @param offsetSize the size of the offset field in bytes
         * @param lengthSize the size of the length field in bytes
         * @return a new ChunkedStorage instance, or null if parsing fails
         */
        public static ChunkedStorage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int version = Byte.toUnsignedInt(buffer.get());
            int rank = Byte.toUnsignedInt(buffer.get());
            long[] chunkSizes = new long[rank];
            for (int i = 0; i < rank; i++) {
                chunkSizes[i] = Integer.toUnsignedLong(buffer.getInt());
            }
            // Note: The commented-out code suggests address parsing is incomplete
            return null;
        }

        /**
         * Returns a string representation of this ChunkedStorage.
         *
         * @return a string describing the version, rank, chunk sizes, and address
         */
        @Override
        public String toString() {
            return "ChunkedStorage{" +
                    "version=" + version +
                    ", rank=" + rank +
                    ", chunkSizes=" + Arrays.toString(chunkSizes) +
                    ", address=" + (address != null ? address.getInstance(Long.class) : "null") +
                    '}';
        }
    }

    /**
     * Represents contiguous storage properties for a dataset.
     */
    public static class ContiguousStorage {
        private final int version;
        private final HdfFixedPoint address;
        private final HdfFixedPoint size;

        /**
         * Constructs a ContiguousStorage instance.
         *
         * @param version the version of the contiguous storage message
         * @param address the file address of the contiguous data
         * @param size    the size of the contiguous data
         */
        public ContiguousStorage(int version, HdfFixedPoint address, HdfFixedPoint size) {
            this.version = version;
            this.address = address;
            this.size = size;
        }

        /**
         * Parses contiguous storage properties from the provided data.
         *
         * @param flags      message flags
         * @param data       the byte array containing the message data
         * @param offsetSize the size of the offset field in bytes
         * @param lengthSize the size of the length field in bytes
         * @return a new ContiguousStorage instance, or null if parsing fails
         */
        public static ContiguousStorage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int version = Byte.toUnsignedInt(buffer.get());
            BitSet emptyBitSet = new BitSet(0);
            // Note: The commented-out code suggests address and size parsing is incomplete
            return null;
        }

        /**
         * Returns a string representation of this ContiguousStorage.
         *
         * @return a string describing the version, address, and size
         */
        @Override
        public String toString() {
            return "ContiguousStorage{" +
                    "version=" + version +
                    ", address=" + (address != null ? address.getDatatype() : "null") +
                    ", size=" + (size != null ? size.getDatatype() : "null") +
                    '}';
        }
    }
}