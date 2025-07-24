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
public class FillMessage extends HdfMessage {
    private final int size;
    /**
     * The actual fill value data (if defined).
     */
    private final byte[] fillValue;

    /**
     * Constructs a FillValueMessage with the specified components.
     *
     * @param size                the size of the fill value in bytes
     * @param fillValue           the actual fill value data
     * @param flags               message flags
     * @param sizeMessageData     the size of the message data in bytes
     */
    public FillMessage(
            int size,
            byte[] fillValue,
            int flags,
            int sizeMessageData
    ) {
        super(MessageType.FillMessage, sizeMessageData, flags);
        this.size = size;
        this.fillValue = fillValue;
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

        size = buffer.getInt();

        // Parse Fill Value
        fillValue = new byte[size];
        buffer.get(fillValue);

        // Return a constructed instance of FillValueMessage
        return new FillMessage(size, fillValue, flags, (short) data.length);
    }

    /**
     * Returns a string representation of this FillValueMessage.
     *
     * @return a string describing the message size, version, allocation times, and fill value details
     */
    @Override
    public String toString() {
        return "FillMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
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
        // Write Size (unsigned 4 bytes)
        buffer.putInt(size);
        buffer.put(fillValue);
    }
}