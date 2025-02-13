package com.github.karlnicholas.hdf5javalib.message;

import java.nio.ByteBuffer;

public class NilMessage extends HdfMessage {

    public NilMessage() {
        super(MessageType.NilMessage, ()-> (short) 8, (byte)0);
    }

    public static HdfMessage parseHeaderMessage(byte flag, byte[] data, int offsetSize, int lengthSize) {
        // No data to parse for null message
        return new NilMessage();
    }

    @Override
    public String toString() {
        return "NilMessage{}";
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }
}
