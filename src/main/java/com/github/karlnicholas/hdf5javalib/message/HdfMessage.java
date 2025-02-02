package com.github.karlnicholas.hdf5javalib.message;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

@Getter
public abstract class HdfMessage {
    private final short messageType;
    private final short sizeMessageData;
    private final byte messageFlags;

    protected HdfMessage(short messageType, Supplier<Short> sizeSupplier, byte messageFlags) {
        this.messageType = messageType;
        this.sizeMessageData = sizeSupplier.get();
        this.messageFlags = messageFlags;
    }

    protected void writeMessageData(ByteBuffer buffer) {
        buffer.putShort(messageType);
        buffer.putShort(sizeMessageData);
        buffer.put(messageFlags);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
    }

    public abstract void writeToByteBuffer(ByteBuffer buffer);
}
