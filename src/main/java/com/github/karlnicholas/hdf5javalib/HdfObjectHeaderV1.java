package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.message.HdfMessage;
import com.github.karlnicholas.hdf5javalib.message.SymbolTableMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.parseDataObjectHeaderMessages;

public class HdfObjectHeaderV1 {
    private final int version;
    private final int totalHeaderMessages;
    private final long objectReferenceCount;
    private final long objectHeaderSize;
    private final List<HdfMessage> headerMessages;

    public HdfObjectHeaderV1(
            int version,
            int totalHeaderMessages,
            long objectReferenceCount,
            long objectHeaderSize,
            List<HdfMessage> headerMessages
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
        int objectReferenceCount = buffer.getInt();
        int objectHeaderSize = buffer.getInt();
        int reservedInt = buffer.getInt(); // Reserved field (should be 0)
        if (reservedInt != 0) {
            throw new IllegalArgumentException("Invalid reserved field in object header: " + reservedInt);
        }

        // Parse header messages
        List<HdfMessage> headerMessages = new ArrayList<>();
        // Read and parse header messages
        parseDataObjectHeaderMessages(fileChannel, objectHeaderSize, offsetSize, lengthSize, headerMessages);

        return new HdfObjectHeaderV1(version, totalHeaderMessages, objectReferenceCount, objectHeaderSize, headerMessages);
    }

    public SymbolTableMessage getHdfSymbolTableMessage() {
        return (SymbolTableMessage)headerMessages.get(0);
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

    public List<HdfMessage> getHeaderMessages() {
        return headerMessages;
    }

    @Override
    public String toString() {
        return "HdfObjectHeaderV1{" +
                "version=" + version +
                ", totalHeaderMessages=" + totalHeaderMessages +
                ", objectReferenceCount=" + objectReferenceCount +
                ", objectHeaderSize=" + objectHeaderSize +
                ", headerMessages=" + headerMessages +
                '}';
    }
}
