package com.github.karlnicholas.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

@Getter
public abstract class HdfMessage {
    private final MessageType messageType;
    @Setter
    private short sizeMessageData;
    private final byte messageFlags;

    protected HdfMessage(MessageType messageType, Supplier<Short> sizeSupplier, byte messageFlags) {
        this.messageType = messageType;
        this.sizeMessageData = sizeSupplier.get();
        this.messageFlags = messageFlags;
    }

    /**
     * Header Message Type #1 (short)
     * Size of Header Message Data #1 (short)
     * Header Message #1 Flags (byte)
     * Reserved (zero) (3 bytes)
     *
     * @param buffer ByteBuffer
     */
    protected void writeMessageData(ByteBuffer buffer) {
        buffer.putShort(messageType.getValue());
        buffer.putShort(sizeMessageData);
        buffer.put(messageFlags);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
    }

    public abstract void writeToByteBuffer(ByteBuffer buffer);
}
