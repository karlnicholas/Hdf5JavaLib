package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.message.HdfMessage;
import com.github.karlnicholas.hdf5javalib.message.HdfSymbolTableMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class HdfHeaderMessage {
    private final int type;
    private final int size;
    private final int flags;
    private final HdfMessage hdfMessage;

    public HdfHeaderMessage(int type, int size, int flags, HdfMessage hdfMessage) {
        this.type = type;
        this.size = size;
        this.flags = flags;
        this.hdfMessage = hdfMessage;
    }

    /**
     * Parses an HdfHeaderMessage from a FileChannel.
     *
     * @param fileChannel The FileChannel positioned at the start of the Header Message.
     * @param offsetSize  The size of offsets specified in the superblock (in bytes).
     * @return A parsed HdfHeaderMessage instance.
     * @throws IOException If an I/O error occurs while reading from the FileChannel.
     */
    public static HdfHeaderMessage fromFileChannel(FileChannel fileChannel, int offsetSize, int lengthSize) throws IOException {
        // Read the first 10 bytes (type, size, flags, reserved)
        ByteBuffer headerBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerBuffer);
        headerBuffer.flip();

        // Parse the type, size, and flags
        int type = Short.toUnsignedInt(headerBuffer.getShort());
        int size = Short.toUnsignedInt(headerBuffer.getShort());
        byte flags = headerBuffer.get();
        headerBuffer.position(headerBuffer.position() + 3); // Skip 3 reserved bytes

        // Read the data section based on the size
        ByteBuffer dataBuffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(dataBuffer);
        dataBuffer.flip();

        // Parse the message based on its type
        HdfMessage parsedMessage = null;
        if (type == 0x0011) { // Symbol Table Message
            // Wrap the data bytes in a new ByteBuffer with little-endian order
            parsedMessage = new HdfSymbolTableMessage().parseHeaderMessage((byte)0, dataBuffer.array(), offsetSize, lengthSize);
        }

        return new HdfHeaderMessage(type, size, flags, parsedMessage);
    }

    public int getType() {
        return type;
    }

    public int getSize() {
        return size;
    }

    public int getFlags() {
        return flags;
    }

    public HdfMessage getHdfMessage() {
        return hdfMessage;
    }

    @Override
    public String toString() {
        String parsedMessageString = (hdfMessage != null) ? hdfMessage.toString() : "Message Type Unknown";
        return "HdfHeaderMessage{" +
                "type=" + type +
                ", size=" + size +
                ", flags=" + flagsToString() +
                ", parsedMessage=" + parsedMessageString +
                '}';
    }

    private String flagsToString() {
        StringBuilder sb = new StringBuilder();
        sb.append(flags).append(" (");
        if ((flags & 0x01) != 0) sb.append("Constant, ");
        if ((flags & 0x02) != 0) sb.append("Shared, ");
        if ((flags & 0x04) != 0) sb.append("Not Shareable, ");
        if ((flags & 0x08) != 0) sb.append("Fail on Unknown, ");
        if ((flags & 0x10) != 0) sb.append("Invalidate, ");
        if ((flags & 0x20) != 0) sb.append("Modified by Unknown, ");
        if ((flags & 0x40) != 0) sb.append("Shareable, ");
        if ((flags & 0x80) != 0) sb.append("Fail Always, ");
        if (sb.length() > 3) sb.setLength(sb.length() - 2); // Remove trailing comma and space
        sb.append(")");
        return sb.toString();
    }
}
