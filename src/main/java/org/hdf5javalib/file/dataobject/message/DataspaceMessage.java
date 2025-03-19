package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

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
