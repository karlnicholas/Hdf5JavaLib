package org.hdf5javalib.redo.hdffile.dataobjects.messages;

import org.hdf5javalib.redo.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    /** The version of the fill value message format. */
    private final int version;
    /** The time when space for fill values is allocated (Early, Late, Incremental). */
    private final int spaceAllocationTime;
    /** The time when the fill value is written (on creation or first write). */
    private final int fillValueWriteTime;
    /** Indicates if a user-defined fill value is provided (0: undefined, 1: defined). */
    private final int fillValueDefined;
    /** The size of the fill value in bytes (if defined). */
    private final int size;
    /** The actual fill value data (if defined). */
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
            byte flags,
            short sizeMessageData
    ) {
        super(MessageType.FillValueMessage, sizeMessageData, flags);
        this.version = version;
        this.spaceAllocationTime = spaceAllocationTime;
        this.fillValueWriteTime = fillValueWriteTime;
        this.fillValueDefined = fillValueDefined;
        this.size = size;
        this.fillValue = fillValue;
    }

    /**
     * Parses a FillValueMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new FillValueMessage instance parsed from the data
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse the first 4 bytes
        int version = Byte.toUnsignedInt(buffer.get());
        int spaceAllocationTime = Byte.toUnsignedInt(buffer.get());
        int fillValueWriteTime = Byte.toUnsignedInt(buffer.get());
        int fillValueDefined = Byte.toUnsignedInt(buffer.get());

        // Initialize optional fields
        int size = 0;
        byte[] fillValue = null;

        // Handle Version 2+ behavior and fillValueDefined flag
        if (version >= 2 && fillValueDefined == 1) {
            // Parse Size (unsigned 4 bytes)
            size = buffer.getInt();

            // Parse Fill Value
            fillValue = new byte[size];
            buffer.get(fillValue);
        }

        // Return a constructed instance of FillValueMessage
        return new FillValueMessage(version, spaceAllocationTime, fillValueWriteTime, fillValueDefined, size, fillValue, flags, (short) data.length);
    }

    /**
     * Returns a string representation of this FillValueMessage.
     *
     * @return a string describing the message size, version, allocation times, and fill value details
     */
    @Override
    public String toString() {
        return "FillValueMessage("+(getSizeMessageData()+8)+"){" +
                "version=" + version +
                ", spaceAllocationTime=" + spaceAllocationTime +
                ", fillValueWriteTime=" + fillValueWriteTime +
                ", fillValueDefined=" + fillValueDefined +
                ", size=" + size +
                ", fillValue=" + (fillValue != null ? fillValue.length + " bytes" : "undefined") +
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