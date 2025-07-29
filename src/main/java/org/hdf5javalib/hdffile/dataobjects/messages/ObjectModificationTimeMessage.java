package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.hdfjava.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Represents an Object Modification Time Message in the HDF5 file format.
 * <p>
 * The {@code ObjectModificationTimeMessage} class records the last modification
 * timestamp of an object within an HDF5 file. This timestamp indicates when the
 * object (such as a dataset, group, or committed datatype) was last modified.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the modification time message format.</li>
 *   <li><b>Reserved (3 bytes)</b>: Unused bytes for alignment.</li>
 *   <li><b>Timestamp (4 bytes)</b>: The modification time as a UNIX timestamp
 *       (seconds since January 1, 1970, 00:00:00 UTC).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Tracking when an HDF5 object was last updated.</li>
 *   <li>Supporting file versioning and change detection.</li>
 *   <li>Providing metadata for data management and auditing.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class ObjectModificationTimeMessage extends HdfMessage {
    private static final int OBJECTMODIFICATIONTIME_RESERVED_1 = 3;
    /**
     * The version of the modification time message format.
     */
    private final int version;
    /**
     * The modification time as seconds since the UNIX epoch.
     */
    private final long secondsAfterEpoch;

    /**
     * Constructs an ObjectModificationTimeMessage with the specified components.
     *
     * @param version           the version of the message format
     * @param secondsAfterEpoch the modification time as seconds since the UNIX epoch
     * @param flags             message flags
     * @param sizeMessageData   the size of the message data in bytes
     */
    public ObjectModificationTimeMessage(int version, long secondsAfterEpoch, int flags, int sizeMessageData) {
        super(MessageType.OBJECT_MODIFICATION_TIME_MESSAGE, sizeMessageData, flags);
        this.version = version;
        this.secondsAfterEpoch = secondsAfterEpoch;
    }

    /**
     * Parses an ObjectModificationTimeMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new ObjectModificationTimeMessage instance
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse version
        int version = Byte.toUnsignedInt(buffer.get());

        // Skip reserved bytes
        buffer.position(buffer.position() + OBJECTMODIFICATIONTIME_RESERVED_1);

        // Parse seconds after UNIX epoch
        long secondsAfterEpoch = Integer.toUnsignedLong(buffer.getInt());

        // Return a constructed instance of ObjectModificationTimeMessage
        return new ObjectModificationTimeMessage(version, secondsAfterEpoch, flags, data.length);
    }

    /**
     * Returns a string representation of this ObjectModificationTimeMessage.
     *
     * @return a string describing the message size, version, and timestamp
     */
    @Override
    public String toString() {
        return "ObjectModificationTimeMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", secondsAfterEpoch=" + Instant.ofEpochSecond(secondsAfterEpoch).toString() +
                '}';
    }

    /**
     * Writes the ObjectModificationTimeMessage data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put((byte) version);
        // Write reserved bytes
        buffer.position(buffer.position() + OBJECTMODIFICATIONTIME_RESERVED_1);
        // Write seconds after UNIX epoch
        buffer.putInt((int) secondsAfterEpoch);
    }
}