package com.github.karlnicholas.hdf5javalib.message;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ObjectModificationTimeMessage implements HdfMessage {
    private int version;
    private long secondsAfterEpoch;

    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse version
        this.version = Byte.toUnsignedInt(buffer.get());

        // Skip reserved byte
        buffer.get();

        // Parse seconds after UNIX epoch
        this.secondsAfterEpoch = Integer.toUnsignedLong(buffer.getInt());

        return this;
    }

    public int getVersion() {
        return version;
    }

    public long getSecondsAfterEpoch() {
        return secondsAfterEpoch;
    }

    @Override
    public String toString() {
        return "ObjectModificationTimeMessage{" +
                "version=" + version +
                ", secondsAfterEpoch=" + secondsAfterEpoch +
                '}';
    }
}
