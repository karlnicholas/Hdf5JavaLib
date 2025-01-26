package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.message.HdfSymbolTableMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class HdfObjectHeaderV1 {
    private final int version;
    private final int totalHeaderMessages;
    private final long objectReferenceCount;
    private final long objectHeaderSize;
    private final List<HdfHeaderMessage> headerMessages;

    private HdfObjectHeaderV1(
            int version,
            int totalHeaderMessages,
            long objectReferenceCount,
            long objectHeaderSize,
            List<HdfHeaderMessage> headerMessages
    ) {
        this.version = version;
        this.totalHeaderMessages = totalHeaderMessages;
        this.objectReferenceCount = objectReferenceCount;
        this.objectHeaderSize = objectHeaderSize;
        this.headerMessages = headerMessages;
    }

    /**
     * Reads an HdfObjectHeaderV1 from a FileChannel.
     *
     * @param fileChannel        The FileChannel to read from.
     * @param objectHeaderAddress The address of the object header in the file.
     * @param offsetSize          The size of offsets (in bytes).
     * @return A parsed HdfObjectHeaderV1 instance.
     * @throws IOException If an I/O error occurs.
     */
    public static HdfObjectHeaderV1 readFromFileChannel(FileChannel fileChannel, long objectHeaderAddress, int offsetSize, int lengthSize) throws IOException {
        // Navigate to the object header address
        fileChannel.position(objectHeaderAddress);

        // Read the object header's fixed-size metadata (16 bytes)
        ByteBuffer buffer = ByteBuffer.allocate(16).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();

        // Parse metadata
        int version = Byte.toUnsignedInt(buffer.get());
        byte reserved = buffer.get(); // Reserved byte (should be 0)
        if (reserved != 0) {
            throw new IllegalArgumentException("Invalid reserved byte in object header: " + reserved);
        }
        int totalHeaderMessages = Short.toUnsignedInt(buffer.getShort());
        long objectReferenceCount = Integer.toUnsignedLong(buffer.getInt());
        long objectHeaderSize = Integer.toUnsignedLong(buffer.getInt());
        int reservedInt = buffer.getInt(); // Reserved field (should be 0)
        if (reservedInt != 0) {
            throw new IllegalArgumentException("Invalid reserved field in object header: " + reservedInt);
        }

        // Parse header messages
        List<HdfHeaderMessage> headerMessages = new ArrayList<>();
        long bytesRemaining = objectHeaderSize;
        for (int i = 0; i < totalHeaderMessages; i++) {
            if (bytesRemaining <= 0) {
                break;
            }

            // Read and parse a header message
            HdfHeaderMessage message = HdfHeaderMessage.fromFileChannel(fileChannel, offsetSize, lengthSize);
            headerMessages.add(message);

            // Adjust bytesRemaining and align to 8-byte boundary
            int messageSize = 4 + message.getSize(); // 4 bytes for type, size, flags
            int padding = (8 - (messageSize % 8)) % 8;
            bytesRemaining -= (messageSize + padding);

            // Skip padding bytes
            if (padding > 0) {
                fileChannel.position(fileChannel.position() + padding);
            }
        }

        return new HdfObjectHeaderV1(version, totalHeaderMessages, objectReferenceCount, objectHeaderSize, headerMessages);
    }

    public HdfSymbolTableMessage getHdfSymbolTableMessage() {
        return (HdfSymbolTableMessage)headerMessages.get(0).getHdfMessage();
    }

    public int getVersion() {
        return version;
    }

    public int getTotalHeaderMessages() {
        return totalHeaderMessages;
    }

    public long getObjectReferenceCount() {
        return objectReferenceCount;
    }

    public long getObjectHeaderSize() {
        return objectHeaderSize;
    }

    public List<HdfHeaderMessage> getHeaderMessages() {
        return headerMessages;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfObjectHeaderV1{")
                .append("version=").append(version)
                .append(", totalHeaderMessages=").append(totalHeaderMessages)
                .append(", objectReferenceCount=").append(objectReferenceCount)
                .append(", objectHeaderSize=").append(objectHeaderSize)
                .append(", headerMessages=").append(headerMessages)
                .append('}');
        return sb.toString();
    }
}
