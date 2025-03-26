package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

/**
 * Represents an Object Modification Time Message in the HDF5 file format.
 *
 * <p>The Object Modification Time Message records the last modification timestamp
 * of an object within an HDF5 file. This timestamp indicates when the object
 * (such as a dataset, group, or committed datatype) was last modified.</p>
 *
 * <h2>Structure</h2>
 * <p>The Object Modification Time Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the modification time message format.</li>
 *   <li><b>Timestamp (8 bytes)</b>: Stores the modification time as a UNIX timestamp
 *       (seconds since January 1, 1970, 00:00:00 UTC).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>The Object Modification Time Message is useful for:</p>
 * <ul>
 *   <li>Tracking when an HDF5 object was last updated.</li>
 *   <li>Supporting file versioning and change detection.</li>
 *   <li>Providing metadata for data management and auditing.</li>
 * </ul>
 *
 * <h2>Processing</h2>
 * <p>If this message is present in an object's metadata, the stored timestamp
 * can be extracted and converted to a human-readable date/time format.
 * If absent, the modification time may not be explicitly tracked for the object.</p>
 *
 * <p>This class provides methods to parse and interpret Object Modification Time Messages
 * based on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___o_b_j_e_c_t___h_e_a_d_e_r.html">
 *      HDF5 Object Header Documentation</a>
 */
@Getter
public class ObjectModificationTimeMessage extends HdfMessage {
    private final int version;
    private final long secondsAfterEpoch;

    // Constructor to initialize all fields
    public ObjectModificationTimeMessage(int version, long secondsAfterEpoch, byte flags, short sizeMessageData) {
        super(MessageType.ObjectModificationTimeMessage, sizeMessageData, flags);
        this.version = version;
        this.secondsAfterEpoch = secondsAfterEpoch;
    }

    /**
     * Parses the header message and returns a constructed instance.
     *
     * @param flags      Flags associated with the message (not used here).
     * @param data       Byte array containing the header message data.
     * @param offsetSize Size of offsets in bytes (not used here).
     * @param lengthSize Size of lengths in bytes (not used here).
     * @return A fully constructed `ObjectModificationTimeMessage` instance.
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse version
        int version = Byte.toUnsignedInt(buffer.get());

        // Skip reserved bytes
        buffer.position(buffer.position() + 3);

        // Parse seconds after UNIX epoch
        long secondsAfterEpoch = Integer.toUnsignedLong(buffer.getInt());

        // Return a constructed instance of ObjectModificationTimeMessage
        return new ObjectModificationTimeMessage(version, secondsAfterEpoch, flags, (short)data.length);
    }

    @Override
    public String toString() {
        return "ObjectModificationTimeMessage("+(getSizeMessageData()+8)+"){" +
                "version=" + version +
                ", secondsAfterEpoch=" + Instant.ofEpochSecond(secondsAfterEpoch).toString() +
                '}';
    }

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put((byte) version);
        // Skip reserved bytes
        buffer.position(buffer.position() + 3);
        // Parse seconds after UNIX epoch
        buffer.putInt((int) secondsAfterEpoch);
    }
}
