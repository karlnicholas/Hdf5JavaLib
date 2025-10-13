package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.hdfjava.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Represents a Fill Value Message in the HDF5 file format.
 * <p>
 * The {@code FillValueMessage} class defines the default value used to initialize
 * unallocated or uninitialized elements of a dataset. This ensures that datasets
 * have a consistent default value when accessed before explicit data is written.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the fill value format.</li>
 *   <li><b>Space Allocation Time (1 byte, version-dependent)</b>: When space for
 *       fill values is allocated (Early, Late, or Incremental).</li>
 *   <li><b>Fill Value Write Time (1 byte, version-dependent)</b>: When the fill
 *       value is written (on creation or first write).</li>
 *   <li><b>Fill Value Defined Flag (1 byte)</b>: Indicates if a user-defined fill
 *       value is provided.</li>
 *   <li><b>Fill Value Size (4 bytes, optional)</b>: The size of the fill value in bytes.</li>
 *   <li><b>Fill Value Data (variable, optional)</b>: The actual fill value, matching
 *       the dataset's datatype.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <ul>
 *   <li>Defining default values for missing or newly allocated data.</li>
 *   <li>Ensuring consistency when reading uninitialized portions of a dataset.</li>
 *   <li>Improving dataset integrity in applications requiring structured default data.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class FillValueMessage extends HdfMessage {
    /**
     * The version of the fill value message format.
     */
    private final int version;
    /**
     * The time when space for fill values is allocated (Early, Late, Incremental).
     */
    private final int spaceAllocationTime;
    /**
     * The time when the fill value is written (on creation or first write).
     */
    private final int fillValueWriteTime;
    /**
     * Indicates if a user-defined fill value is provided (0: undefined, 1: defined).
     */
    private final int fillValueDefined;
    /**
     * The size of the fill value in bytes (if defined).
     */
    private final int size;
    /**
     * The actual fill value data (if defined).
     */
    private final byte[] fillValue;

    /**
     * Constructs a FillValueMessage with the specified components.
     *
     * @param version             the version of the fill value message format
     * @param spaceAllocationTime the time when space for fill values is allocated
     * @param fillValueWriteTime  the time when the fill value is written
     * @param fillValueDefined    indicates if a user-defined fill value is provided
     * @param size                the size of the fill value in bytes
     * @param fillValue           the actual fill value data
     * @param flags               message flags
     * @param sizeMessageData     the size of the message data in bytes
     */
    public FillValueMessage(
            int version,
            int spaceAllocationTime,
            int fillValueWriteTime,
            int fillValueDefined,
            int size,
            byte[] fillValue,
            int flags,
            int sizeMessageData
    ) {
        super(MessageType.FILL_VALUE_MESSAGE, sizeMessageData, flags);
        this.version = version;
        this.spaceAllocationTime = spaceAllocationTime;
        this.fillValueWriteTime = fillValueWriteTime;
        this.fillValueDefined = fillValueDefined;
        this.size = size;
        this.fillValue = fillValue;
    }

    public int getFillValueDefined() {
        return fillValueDefined;
    }

    public int getSize() {
        return size;
    }

    public byte[] getFillValue() {
        return fillValue;
    }

    /**
     * Parses a FillValueMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new FillValueMessage instance parsed from the data
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Initialize optional fields
        int size = 0;
        byte[] fillValue = null;
        int spaceAllocationTime = -1;
        int fillValueWriteTime = -1;
        int fillValueDefined;


        // Parse the first 4 bytes
        int version = Byte.toUnsignedInt(buffer.get());
        if (version < 3) {
            spaceAllocationTime = Byte.toUnsignedInt(buffer.get());
            fillValueWriteTime = Byte.toUnsignedInt(buffer.get());
            fillValueDefined = Byte.toUnsignedInt(buffer.get());

            // Handle Version 2+ behavior and fillValueDefined flag
            if (version >= 2 && fillValueDefined == 1) {
                // Parse Size (unsigned 4 bytes)
                size = buffer.getInt();

                // Parse Fill Value
                fillValue = new byte[size];
                buffer.get(fillValue);
            }

            // Return a constructed instance of FillValueMessage
            return new FillValueMessage(version, spaceAllocationTime, fillValueWriteTime, fillValueDefined, size, fillValue, flags, data.length);

        } else {
            final int FILL_VALUE_DEFINED_MASK = 0x20;    // Bit 5// Read Flags (1 byte)

            int fvFlags = buffer.get();

            // Check if Fill Value Defined flag is set (bit 5)
            fillValueDefined = (fvFlags & FILL_VALUE_DEFINED_MASK);

            if (fillValueDefined != 0) {
                // Read Size (4 bytes, assuming 32-bit integer as per HDF5 convention for size fields)
                size = buffer.getInt();
                if (size < 0) {
                    throw new IllegalArgumentException("Invalid fill value size: " + size);
                }
                // Read Fill Value (variable size)
                fillValue = new byte[Math.toIntExact(size)];
                buffer.get(fillValue);
            } else {
                size = 0;
                fillValue = null;
            }            // Return a constructed instance of FillValueMessage
            return new FillValueMessage(version, spaceAllocationTime, fillValueWriteTime, fillValueDefined, size, fillValue, flags, data.length);

        }
    }

    /**
     * Returns a string representation of this FillValueMessage.
     *
     * @return a string describing the message size, version, allocation times, and fill value details
     */
    @Override
    public String toString() {
        return "FillValueMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", spaceAllocationTime=" + spaceAllocationTime +
                ", fillValueWriteTime=" + fillValueWriteTime +
                ", fillValueDefined=" + fillValueDefined +
                ", size=" + size +
                ", fillValue=" + (fillValue != null ? Arrays.toString(fillValue) : "undefined") +
                '}';
    }

    /**
     * Writes the fill value message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write the first 4 bytes
        buffer.put((byte) version);
        buffer.put((byte) spaceAllocationTime);
        buffer.put((byte) fillValueWriteTime);
        buffer.put((byte) fillValueDefined);

        // Handle Version 2+ behavior and fillValueDefined flag
        if (version >= 2 && fillValueDefined == 1) {
            // Write Size (unsigned 4 bytes)
            buffer.putInt(size);
            buffer.put(fillValue);
        }
    }
}