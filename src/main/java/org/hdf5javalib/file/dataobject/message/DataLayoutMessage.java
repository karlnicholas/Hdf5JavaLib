package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import lombok.Setter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Data Layout Message in the HDF5 file format.
 *
 * <p>The Data Layout Message defines how raw data is stored within an HDF5 file.
 * It specifies the data storage method (contiguous, chunked, or compact) and
 * includes details about data organization, offsets, and dimensions.</p>
 *
 * <h2>Structure</h2>
 * <p>The Data Layout Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the data layout message format.</li>
 *   <li><b>Layout Class (1 byte)</b>: Defines the storage method, which can be one of:
 *       <ul>
 *         <li><b>Contiguous (0)</b>: Data is stored in a single continuous block.</li>
 *         <li><b>Chunked (1)</b>: Data is divided into fixed-size chunks, allowing
 *             partial I/O and compression.</li>
 *         <li><b>Compact (2)</b>: Small datasets are stored directly within the
 *             object header.</li>
 *       </ul>
 *   </li>
 *   <li><b>Storage Properties</b>: Additional fields vary based on the layout class:
 *       <ul>
 *         <li><b>Contiguous:</b> Includes an 8-byte file address indicating where
 *             the data begins.</li>
 *         <li><b>Chunked:</b> Includes the dimensionality and chunk dimensions,
 *             along with a file address for each chunk.</li>
 *         <li><b>Compact:</b> Stores raw data directly within the message.</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h2>Layout Classes</h2>
 * <p>The HDF5 format supports different layout strategies:</p>
 * <ul>
 *   <li><b>Contiguous</b>: Efficient for sequential access but not ideal for partial modifications.</li>
 *   <li><b>Chunked</b>: Supports compression, partial I/O, and expansion, making it suitable
 *       for large and multidimensional datasets.</li>
 *   <li><b>Compact</b>: Optimized for very small datasets, reducing file overhead.</li>
 * </ul>
 *
 * <p>This class provides methods for parsing and interpreting Data Layout Messages
 * based on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___d_a_t_a_l_a_y_o_u_t.html">
 *      HDF5 Data Layout Documentation</a>
 */
@Getter
public class DataLayoutMessage extends HdfMessage {
    private final int version;
    private final int layoutClass;
    @Setter
    private HdfFixedPoint dataAddress;
    private final HdfFixedPoint[] dimensionSizes;
    private final int compactDataSize;
    private final byte[] compactData;
    private final HdfFixedPoint datasetElementSize;

    // Constructor to initialize all fields
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
     * Parses the header message and returns a constructed instance.
     *
     * @param flags       Flags associated with the message (not used here).
     * @param data        Byte array containing the header message data.
     * @param hdfDataFile
     * @return A fully constructed `DataLayoutMessage` instance.
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
                dataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);
                dimensionSizes = new HdfFixedPoint[1];
                dimensionSizes[0] = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);
                break;

            case 2: // Chunked Storage
                dataAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);
                int numDimensions = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                dimensionSizes = new HdfFixedPoint[numDimensions];
                for (int i = 0; i < numDimensions; i++) {
                    dimensionSizes[i] = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);
                }
                // Dataset element size (4 bytes)
                FixedPointDatatype fourByteFixedPoibtDatatype = new FixedPointDatatype(
                        FixedPointDatatype.createClassAndVersion(),
                        FixedPointDatatype.createClassBitField(false, false, false, false),
                        4, (short) 0, (short) (4*8)
                );
                byte[] fourByteBytes = new byte[4];
                buffer.get(fourByteBytes);
                datasetElementSize = fourByteFixedPoibtDatatype.getInstance(HdfFixedPoint.class, fourByteBytes);
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }

        // Return a constructed instance of DataLayoutMessage
        return new DataLayoutMessage(version, layoutClass, dataAddress, dimensionSizes, compactDataSize, compactData, datasetElementSize, flags, (short)data.length);
    }

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

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Read version (1 byte)
        buffer.put((byte) version);
        // Read layout class (1 byte)
        buffer.put((byte) layoutClass);

        switch (layoutClass) {
            case 0: // Compact Storage
                buffer.putShort((short) compactDataSize); // Compact data size (2 bytes)
                buffer.put(compactData); // Read compact data
                break;

            case 1: // Contiguous Storage
                writeFixedPointToBuffer(buffer, dataAddress);
                writeFixedPointToBuffer(buffer, dimensionSizes[0]);
                break;

            case 2: // Chunked Storage
                writeFixedPointToBuffer(buffer, dataAddress);
                buffer.get(dimensionSizes.length); // Number of dimensions (1 byte)
                for (HdfFixedPoint dimensionSize: dimensionSizes) {
                    writeFixedPointToBuffer(buffer, dimensionSize);
                }
                writeFixedPointToBuffer(buffer, datasetElementSize);
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }

    }

    public static class ChunkedStorage {
        private final int version;
        private final int rank;
        private final long[] chunkSizes;
        private final HdfFixedPoint address;

        public ChunkedStorage(int version, int rank, long[] chunkSizes, HdfFixedPoint address) {
            this.version = version;
            this.rank = rank;
            this.chunkSizes = chunkSizes;
            this.address = address;
        }

        public static ChunkedStorage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int version = Byte.toUnsignedInt(buffer.get());
            int rank = Byte.toUnsignedInt(buffer.get());
            long[] chunkSizes = new long[rank];
            for (int i = 0; i < rank; i++) {
                chunkSizes[i] = Integer.toUnsignedLong(buffer.getInt());
            }
//            HdfFixedPoint address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
//            return new ChunkedStorage(version, rank, chunkSizes, address);
            return null;
        }

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

    public static class ContiguousStorage {
        private final int version;
        private final HdfFixedPoint address;
        private final HdfFixedPoint size;

        public ContiguousStorage(int version, HdfFixedPoint address, HdfFixedPoint size) {
            this.version = version;
            this.address = address;
            this.size = size;
        }

        public static ContiguousStorage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int version = Byte.toUnsignedInt(buffer.get());
            BitSet emptyBitSet = new BitSet(0);
//            HdfFixedPoint address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8));
//            HdfFixedPoint size = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short)0, (short)(lengthSize*8));
//            return new ContiguousStorage(version, address, size);
            return null;
        }

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
