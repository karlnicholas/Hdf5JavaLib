package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Dataspace Message in the HDF5 file format.
 * <p>
 * The {@code DataspaceMessage} class defines the shape and size of a dataset or attribute
 * in an HDF5 file. It specifies the dimensionality (rank), the size of each dimension,
 * and optionally the maximum dimensions for extensible datasets.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the dataspace message format.</li>
 *   <li><b>Dimensionality (1 byte)</b>: The number of dimensions (rank).</li>
 *   <li><b>Flags (1 byte)</b>: Indicates the presence of maximum dimensions or permutation indices.</li>
 *   <li><b>Dimensions</b>: An array of dimension sizes (variable length).</li>
 *   <li><b>Maximum Dimensions</b>: An optional array of maximum dimension sizes (variable length).</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class DataspaceMessage extends HdfMessage {
    private static final int DATASPACE_MESSAGE_RESERVED_1 = 5;
    /**
     * The version of the dataspace message format.
     */
    private final int version;
    /**
     * The number of dimensions (rank).
     */
    private final int dimensionality;
    /**
     * A BitSet indicating the presence of maximum dimensions or permutation indices.
     */
    private final BitSet flags;
    /**
     * The sizes of each dimension.
     */
    private final HdfFixedPoint[] dimensions;
    /**
     * The maximum sizes of each dimension, if present.
     */
    private final HdfFixedPoint[] maxDimensions;
    /**
     * Indicates whether maximum dimensions are present.
     */
    private final boolean hasMaxDimensions;

    /**
     * Constructs a DataspaceMessage with the specified components.
     *
     * @param version          the version of the dataspace message format
     * @param dimensionality   the number of dimensions (rank)
     * @param flags            a BitSet indicating the presence of maximum dimensions or permutation indices
     * @param dimensions       the sizes of each dimension
     * @param maxDimensions    the maximum sizes of each dimension, if present
     * @param hasMaxDimensions indicates whether maximum dimensions are present
     * @param rawFlags         raw message flags
     * @param sizeMessageData  the size of the message data in bytes
     */
    public DataspaceMessage(
            int version,
            int dimensionality,
            BitSet flags,
            HdfFixedPoint[] dimensions,
            HdfFixedPoint[] maxDimensions,
            boolean hasMaxDimensions,
            int rawFlags,
            int sizeMessageData
    ) {
        super(MessageType.DATASPACE_MESSAGE, sizeMessageData, rawFlags);
        this.version = version;
        this.dimensionality = dimensionality;
        this.flags = flags;
        this.dimensions = dimensions;
        this.maxDimensions = maxDimensions;
        this.hasMaxDimensions = hasMaxDimensions;
    }

    /**
     * Parses a DataspaceMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for datatype resources
     * @return a new DataspaceMessage instance parsed from the data
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        int version = Byte.toUnsignedInt(buffer.get());
        int dimensionality = Byte.toUnsignedInt(buffer.get());
        int flagByte = Byte.toUnsignedInt(buffer.get());
        BitSet flagSet = BitSet.valueOf(new byte[]{(byte) flagByte});

        buffer.position(buffer.position() + DATASPACE_MESSAGE_RESERVED_1);

        HdfFixedPoint[] dimensions = new HdfFixedPoint[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensions[i] = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), buffer);
        }

        boolean hasMaxDimensions = flagSet.get(DataspaceFlag.MAX_DIMENSIONS_PRESENT.getBitIndex());
        HdfFixedPoint[] maxDimensions = null;
        if (hasMaxDimensions) {
            maxDimensions = new HdfFixedPoint[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                maxDimensions[i] = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), buffer);
            }
        }

        return new DataspaceMessage(version, dimensionality, flagSet, dimensions, maxDimensions, hasMaxDimensions, flags, data.length);
    }

    /**
     * Returns a string representation of this DataspaceMessage.
     *
     * @return a string describing the message size, version, dimensionality, flags, and dimensions
     */
    @Override
    public String toString() {
        return "DataspaceMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", dimensionality=" + dimensionality +
                ", flags=" + flags +
                ", dimensions=" + HdfDisplayUtils.undefinedArrayToString(dimensions) +
                ", maxDimensions=" + HdfDisplayUtils.undefinedArrayToString(maxDimensions) +
                ", hasMaxDimensions=" + hasMaxDimensions +
                '}';
    }

    /**
     * Writes the dataspace message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        writeInfoToByteBuffer(buffer);
    }

    /**
     * Writes the dataspace message information to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the information to
     */
    public void writeInfoToByteBuffer(ByteBuffer buffer) {
        buffer.put((byte) version);
        buffer.put((byte) dimensionality);

        // Encode BitSet as single byte
        byte encoded = flags.isEmpty() ? 0 : flags.toByteArray()[0];
        buffer.put(encoded);

        buffer.put(new byte[5]);

        if (dimensions != null) {
            for (HdfFixedPoint dimension : dimensions) {
                writeFixedPointToBuffer(buffer, dimension);
            }
        }

        if (maxDimensions != null) {
            for (HdfFixedPoint maxDimension : maxDimensions) {
                writeFixedPointToBuffer(buffer, maxDimension);
            }
        }
    }

    /**
     * Checks if the specified dataspace flag is set.
     *
     * @param flag the DataspaceFlag to check
     * @return true if the flag is set, false otherwise
     */
    public boolean hasFlag(DataspaceFlag flag) {
        return flags.get(flag.getBitIndex());
    }

    /**
     * Builds a BitSet representing the dataspace flags.
     *
     * @param maxDimensionsPresent      true if maximum dimensions are present
     * @param permutationIndicesPresent true if permutation indices are present
     * @return a BitSet encoding the specified flags
     */
    public static BitSet buildFlagSet(boolean maxDimensionsPresent, boolean permutationIndicesPresent) {
        BitSet bits = new BitSet();
        if (maxDimensionsPresent) {
            bits.set(DataspaceFlag.MAX_DIMENSIONS_PRESENT.getBitIndex());
        }
        if (permutationIndicesPresent) {
            bits.set(DataspaceFlag.PERMUTATION_INDICES_PRESENT.getBitIndex());
        }
        return bits;
    }

    public HdfFixedPoint[] getDimensions() {
        return dimensions;
    }

    public int getDimensionality() {
        return dimensionality;
    }

    /**
     * Enum representing flags for HDF5 dataspace messages.
     */
    public enum DataspaceFlag {
        /**
         * Indicates that maximum dimensions are present.
         */
        MAX_DIMENSIONS_PRESENT(0),

        /**
         * Indicates that permutation indices are present.
         */
        PERMUTATION_INDICES_PRESENT(1);

        private final int bitIndex;

        DataspaceFlag(int bitIndex) {
            this.bitIndex = bitIndex;
        }

        /**
         * Gets the bit index of the flag.
         *
         * @return the bit index
         */
        public int getBitIndex() {
            return bitIndex;
        }

        /**
         * Checks if the flag is set in the provided BitSet.
         *
         * @param flags the BitSet containing flag information
         * @return true if the flag is set, false otherwise
         */
        public boolean isSet(BitSet flags) {
            return flags.get(bitIndex);
        }
    }
}