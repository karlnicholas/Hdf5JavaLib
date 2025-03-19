package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Dataspace Message in the HDF5 file format.
 *
 * <p>The Dataspace Message defines the dimensionality and shape of a dataset,
 * specifying how data is organized within an HDF5 file. It includes information
 * about the number of dimensions, the size of each dimension, and optionally
 * maximum dimensions for extensible datasets.</p>
 *
 * <h2>Structure</h2>
 * <p>The Dataspace Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the dataspace format.</li>
 *   <li><b>Dimensionality (1 byte)</b>: Specifies the number of dimensions (rank)
 *       of the dataset, ranging from 0 (scalar) to a maximum allowed value (typically 32).</li>
 *   <li><b>Flags (1 byte, version-dependent)</b>: May indicate whether maximum dimensions
 *       or permutation indexes are present.</li>
 *   <li><b>Dimension Sizes (array of unsigned 64-bit integers)</b>: Defines the
 *       current sizes of each dimension in the dataset.</li>
 *   <li><b>Maximum Dimension Sizes (optional, array of unsigned 64-bit integers)</b>:
 *       Specifies the maximum sizes for extensible dimensions (if applicable).</li>
 * </ul>
 *
 * <h2>Dataspace Types</h2>
 * <p>The HDF5 format supports the following dataspace types:</p>
 * <ul>
 *   <li><b>Scalar</b>: A single data point with no dimensions.</li>
 *   <li><b>Simple</b>: A multidimensional array with fixed or extensible dimensions.</li>
 *   <li><b>Null</b>: A dataspace that contains no elements.</li>
 * </ul>
 *
 * <h2>Extensibility</h2>
 * <p>If a dataspace has maximum dimension sizes larger than its current sizes, it can be
 * extended dynamically, allowing datasets to grow over time.</p>
 *
 * <p>This class provides methods to parse and interpret Dataspace Messages based on
 * the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___d_a_t_a_s_p_a_c_e.html">
 *      HDF5 Dataspace Documentation</a>
 */
@Getter
public class DataspaceMessage extends HdfMessage {
    private final int version; // Version of the dataspace message
    private final int dimensionality; // Number of dimensions (rank)
    private final int flags;
    private final HdfFixedPoint[] dimensions; // Sizes of each dimension
    private final HdfFixedPoint[] maxDimensions; // Maximum sizes of each dimension, if specified
    private final boolean hasMaxDimensions; // Indicates if max dimensions are included

    // Constructor to initialize all fields
    public DataspaceMessage(
            int version,
            int dimensionality,
            int flags,
            HdfFixedPoint[] dimensions,
            HdfFixedPoint[] maxDimensions,
            boolean hasMaxDimensions
    ) {
        super(MessageType.DataspaceMessage, ()->{
            short size = 8;
            if ( dimensions != null ) {
                for (HdfFixedPoint dimension : dimensions) {
                    size += dimension.getDatatype().getSize();
                }
            }
            if ( maxDimensions != null ) {
                for (HdfFixedPoint maxDimension : maxDimensions) {
                    size += maxDimension.getDatatype().getSize();
                }
            }
            return size;
        }, hasMaxDimensions?(byte)1:(byte)0);
        this.version = version;
        this.dimensionality = dimensionality;
        this.flags = flags;
        this.dimensions = dimensions;
        this.maxDimensions = maxDimensions;
        this.hasMaxDimensions = hasMaxDimensions;
    }

    /**
     * Parses the header message and returns a constructed instance.
     *
     * @param ignoredFlags      ignored
     * @param data       Byte array containing the header message data.
     * @param ignoredOffsetSize ignored
     * @param lengthSize Size of lengths in bytes.
     * @return A fully constructed `DataspaceMessage` instance.
     */
    public static HdfMessage parseHeaderMessage(byte ignoredFlags, byte[] data, short ignoredOffsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Read the version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());

        // Read the rank (1 byte)
        int dimensionality = Byte.toUnsignedInt(buffer.get());

        // Read flags (1 byte)
        int parsedFlags = Byte.toUnsignedInt(buffer.get());

        // Skip reserved bytes (5 bytes)
        buffer.position(buffer.position() + 5);

        // Read dimensions
        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint[] dimensions = new HdfFixedPoint[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short)(lengthSize*8));
        }

        // Check for maximum dimensions flag and read if present
        boolean hasMaxDimensions = (parsedFlags & 0x01) != 0; // Bit 0 of flags indicates max dimensions
        HdfFixedPoint[] maxDimensions = null;
        if (hasMaxDimensions) {
            maxDimensions = new HdfFixedPoint[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                maxDimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short)(lengthSize*8));
            }
        }

        // Return a constructed instance of DataspaceMessage
        return new DataspaceMessage(version, dimensionality, parsedFlags, dimensions, maxDimensions, hasMaxDimensions);
    }

    @Override
    public String toString() {
        return "DataspaceMessage{" +
                "version=" + version +
                ", dimensionality=" + dimensionality +
                ", flags=" + flags +
                ", dimensions=" + (dimensions != null ? Arrays.toString(dimensions) : "Not Present") +
                ", maxDimensions=" + (maxDimensions != null  ? Arrays.toString(maxDimensions) : "Not Present") +
                ", hasMaxDimensions=" + hasMaxDimensions +
                '}';
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        writeInfoToByteBuffer(buffer);
    }

    public void writeInfoToByteBuffer(ByteBuffer buffer) {
        // Read the version (1 byte)
        buffer.put((byte) version);

        // Read the rank (1 byte)
        buffer.put((byte) dimensionality);

        // Read flags (1 byte)
        buffer.put((byte) flags);

        // Skip reserved bytes (5 bytes)
        buffer.put(new byte[5]);

        // Read dimensions
        if (dimensions != null) {
            for (HdfFixedPoint dimension : dimensions) {
                writeFixedPointToBuffer(buffer, dimension);
            }
        }

        // Check for maximum dimensions and write if present
        if (maxDimensions != null) {
            for (HdfFixedPoint maxDimension: maxDimensions) {
                writeFixedPointToBuffer(buffer, maxDimension);
            }
        }
    }
}
