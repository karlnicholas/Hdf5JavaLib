package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;

@Getter
public class ObjectModificationTimeMessage extends HdfMessage {
    private final int version;
    private final long secondsAfterEpoch;

    // Constructor to initialize all fields
    public ObjectModificationTimeMessage(int version, long secondsAfterEpoch) {
        super(MessageType.ObjectModificationTimeMessage, ()-> (short) (8), (byte)0);
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
        return new ObjectModificationTimeMessage(version, secondsAfterEpoch);
    }

    @Override
    public String toString() {
        return "ObjectModificationTimeMessage{" +
                "version=" + version +
                ", secondsAfterEpoch=" + Instant.ofEpochSecond(secondsAfterEpoch).toString() +
                '}';
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put((byte) version);
        // Skip reserved bytes
        buffer.position(buffer.position() + 3);
        // Parse seconds after UNIX epoch
        buffer.putInt((int) secondsAfterEpoch);
    }
}
