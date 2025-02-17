package com.github.karlnicholas.hdf5javalib.message;

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
