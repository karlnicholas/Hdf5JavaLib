package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

@Getter
public class DataSpaceMessage extends HdfMessage {
    private final int version; // Version of the dataspace message
    private final int dimensionality; // Number of dimensions (rank)
    private final int flags;
    private final HdfFixedPoint[] dimensions; // Sizes of each dimension
    private final HdfFixedPoint[] maxDimensions; // Maximum sizes of each dimension, if specified
    private final boolean hasMaxDimensions; // Indicates if max dimensions are included

    // Constructor to initialize all fields
    public DataSpaceMessage(
            int version,
            int dimensionality,
            int flags,
            HdfFixedPoint[] dimensions,
            HdfFixedPoint[] maxDimensions,
            boolean hasMaxDimensions
    ) {
        super(1, ()->{
            int size = 8;
            for (HdfFixedPoint dimension : dimensions) {
                size += dimension.getSizeMessageData();
            }
            if ( maxDimensions != null ) {
                for (HdfFixedPoint maxDimension : maxDimensions) {
                    size += maxDimension.getSizeMessageData();
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
     * @param flags      Flags associated with the message.
     * @param data       Byte array containing the header message data.
     * @param offsetSize Size of offsets in bytes.
     * @param lengthSize Size of lengths in bytes.
     * @return A fully constructed `DataSpaceMessage` instance.
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
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
        HdfFixedPoint[] dimensions = new HdfFixedPoint[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
        }

        // Check for maximum dimensions flag and read if present
        boolean hasMaxDimensions = (parsedFlags & 0x01) != 0; // Bit 0 of flags indicates max dimensions
        HdfFixedPoint[] maxDimensions = null;
        if (hasMaxDimensions) {
            maxDimensions = new HdfFixedPoint[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                maxDimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
            }
        }

        // Return a constructed instance of DataSpaceMessage
        return new DataSpaceMessage(version, dimensionality, parsedFlags, dimensions, maxDimensions, hasMaxDimensions);
    }

    @Override
    public String toString() {
        return "DataSpaceMessage{" +
                "version=" + version +
                ", dimensionality=" + dimensionality +
                ", flags=" + flags +
                ", dimensions=" + Arrays.toString(dimensions) +
                ", maxDimensions=" + (hasMaxDimensions ? Arrays.toString(maxDimensions) : "Not Present") +
                ", hasMaxDimensions=" + hasMaxDimensions +
                '}';
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer, int offsetSize) {

    }
}
