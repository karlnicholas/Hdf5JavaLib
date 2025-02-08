package com.github.karlnicholas.hdf5javalib.message;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class NullMessage extends HdfMessage {

    public NullMessage() {
        super((short)0, ()-> (short) 8, (byte)0);
    }

    public static HdfMessage parseHeaderMessage(byte flag, byte[] data, int offsetSize, int lengthSize) {
        // No data to parse for null message
        return new NullMessage();
    }

    @Override
    public String toString() {
        return "NullMessage{}";
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }
}
