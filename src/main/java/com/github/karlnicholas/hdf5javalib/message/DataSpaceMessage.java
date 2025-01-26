package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.HdfDataObjectHeader;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class DataSpaceMessage implements HdfMessage {
    private int version; // Version of the dataspace message
    private int dimensionality; // Number of dimensions (rank)
    private int flags;
    private HdfFixedPoint[] dimensions; // Sizes of each dimension
    private HdfFixedPoint[] maxDimensions; // Maximum sizes of each dimension, if specified
    private boolean hasMaxDimensions; // Indicates if max dimensions are included

    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Read the version (1 byte)
        this.version = Byte.toUnsignedInt(buffer.get());

        // Read the rank (1 byte)
        this.dimensionality = Byte.toUnsignedInt(buffer.get());

        // Check for flags (1 byte, in later versions of the spec)
        this.flags = Byte.toUnsignedInt(buffer.get());
        // Skip reserved bytes (if applicable, based on version)
        byte[] reserved = new byte[5];
        buffer.get(reserved);

        // Read dimensions
        this.dimensions = new HdfFixedPoint[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
        }

        // Check for maximum dimensions flag and read if present
        this.hasMaxDimensions = (flags & 0x01) != 0; // Bit 0 of flags indicates max dimensions
        if (this.hasMaxDimensions) {
            this.maxDimensions = new HdfFixedPoint[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                maxDimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
            }
        } else {
            this.maxDimensions = null;
        }

        return this;
    }

    @Override
    public String toString() {
        return "DataSpaceMessage{" +
                "version=" + version +
                ", dimensionality=" + dimensionality +
                ", dimensions=" + Arrays.toString(dimensions) +
                ", maxDimensions=" + (hasMaxDimensions ? Arrays.toString(maxDimensions) : "Not Present") +
                '}';
    }
}
