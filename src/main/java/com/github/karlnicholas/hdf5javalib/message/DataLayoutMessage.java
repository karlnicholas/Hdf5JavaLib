package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.HdfDataObjectHeader;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Represents a Data Layout Message in an HDF5 file.
 * Header Message Name: Data Layout
 * Header Message Type: 0x0008
 */
public class DataLayoutMessage implements HdfMessage {
    private int version; // Version of the data layout message
    private int layoutClass; // Layout class (e.g., compact, contiguous, chunked)
    private HdfFixedPoint dataAddress; // Address of the data in the file (if applicable)
    private HdfFixedPoint[] dimensionSizes; // Sizes of each dimension (if applicable)
    private int compactDataSize; // Size of compact data (for compact storage only)
    private byte[] compactData; // The compact data itself (for compact storage only)
    private HdfFixedPoint datasetElementSize; // Size of each dataset element (for chunked storage only)

    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Read version (1 byte)
        this.version = Byte.toUnsignedInt(buffer.get());
        if (version < 1 || version > 3) {
            throw new IllegalArgumentException("Unsupported Data Layout Message version: " + version);
        }

        // Read layout class (1 byte)
        this.layoutClass = Byte.toUnsignedInt(buffer.get());

        // Parse layout-specific fields based on the layout class
        switch (layoutClass) {
            case 0: // Compact Storage
                parseCompactLayout(buffer);
                break;

            case 1: // Contiguous Storage
                parseContiguousLayout(buffer, offsetSize);
                break;

            case 2: // Chunked Storage
                parseChunkedLayout(buffer, offsetSize);
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }

        return this;
    }

    /**
     * Parses the fields for compact storage layout.
     *
     * @param buffer The ByteBuffer containing the data.
     */
    private void parseCompactLayout(ByteBuffer buffer) {
        // Read Compact Data Size (2 bytes)
        this.compactDataSize = Short.toUnsignedInt(buffer.getShort());

        // Read Compact Data (variable size)
        this.compactData = new byte[compactDataSize];
        buffer.get(this.compactData);
    }

    /**
     * Parses the fields for contiguous storage layout.
     *
     * @param buffer     The ByteBuffer containing the data.
     * @param offsetSize The size of the offsets in bytes (e.g., 4 or 8 bytes).
     */
    private void parseContiguousLayout(ByteBuffer buffer, int offsetSize) {
        // Read Data Address
        this.dataAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);

        // Read Dimension Sizes
        this.dimensionSizes = new HdfFixedPoint[1]; // Version 3 defines only one dimension size for contiguous storage
        this.dimensionSizes[0] = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
    }

    /**
     * Parses the fields for chunked storage layout.
     *
     * @param buffer     The ByteBuffer containing the data.
     * @param offsetSize The size of the offsets in bytes (e.g., 4 or 8 bytes).
     */
    private void parseChunkedLayout(ByteBuffer buffer, int offsetSize) {
        // Read Data Address
        // Read Data Address
        this.dataAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);

        // Read Dimension Sizes
        int numDimensions = Byte.toUnsignedInt(buffer.get());
        this.dimensionSizes = new HdfFixedPoint[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            this.dimensionSizes[i] = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        }

        // Read Dataset Element Size (4 bytes)
        this.datasetElementSize = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);
    }

    /**
     * Reads an offset from the buffer based on the specified offset size.
     *
     * @param buffer     The ByteBuffer containing the data.
     * @param offsetSize The size of the offset (4 bytes for 32-bit, 8 bytes for 64-bit).
     * @return The parsed offset as a long.
     */
    private long readOffset(ByteBuffer buffer, int offsetSize) {
        if (offsetSize == 4) {
            return Integer.toUnsignedLong(buffer.getInt());
        } else if (offsetSize == 8) {
            return buffer.getLong();
        } else {
            throw new IllegalArgumentException("Unsupported offset size: " + offsetSize);
        }
    }

    @Override
    public String toString() {
        return "DataLayoutMessage{" +
                "version=" + version +
                ", layoutClass=" + layoutClass +
                ", dataAddress=" + (layoutClass == 1 || layoutClass == 2 ? dataAddress : "N/A") +
                ", dimensionSizes=" + Arrays.toString(dimensionSizes) +
                ", compactDataSize=" + (layoutClass == 0 ? compactDataSize : "N/A") +
                ", compactData=" + (layoutClass == 0 ? Arrays.toString(compactData) : "N/A") +
                ", datasetElementSize=" + (layoutClass == 2 ? datasetElementSize : "N/A") +
                '}';
    }

    // Getters for the fields
    public int getVersion() {
        return version;
    }

    public int getLayoutClass() {
        return layoutClass;
    }

    public HdfFixedPoint getDataAddress() {
        return dataAddress;
    }

    public HdfFixedPoint[] getDimensionSizes() {
        return dimensionSizes;
    }

    public int getCompactDataSize() {
        return compactDataSize;
    }

    public byte[] getCompactData() {
        return compactData;
    }

    public HdfFixedPoint getDatasetElementSize() {
        return datasetElementSize;
    }
}
