package org.hdf5javalib.file.dataobject.message;

import java.nio.ByteBuffer;

public class NilMessage extends HdfMessage {

    public NilMessage(int size) {
        super(MessageType.NilMessage, ()-> (short) size, (byte)0);
    }

    public static HdfMessage parseHeaderMessage(byte flag, byte[] data, int offsetSize, int lengthSize) {
        // No data to parse for null message
        return new NilMessage(lengthSize);
    }

    @Override
    public String toString() {
        return "NilMessage{" + super.getSizeMessageData() + "}";
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }
}
