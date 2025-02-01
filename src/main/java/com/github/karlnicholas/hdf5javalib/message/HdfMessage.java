package com.github.karlnicholas.hdf5javalib.message;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

@Getter
public abstract class HdfMessage {
    private final int messageType;
    private final int sizeMessageData;
    private final byte messageFlags;

    protected HdfMessage(int messageType, Supplier<Integer> sizeSupplier, byte messageFlags) {
        this.messageType = messageType;
        this.sizeMessageData = sizeSupplier.get();
        this.messageFlags = messageFlags;
    }

    public abstract void writeToByteBuffer(ByteBuffer buffer, int offsetSize);
}
