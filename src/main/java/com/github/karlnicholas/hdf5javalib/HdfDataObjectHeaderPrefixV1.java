package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.message.ContinuationMessage;
import com.github.karlnicholas.hdf5javalib.message.HdfMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.parseContinuationMessage;
import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.parseDataObjectHeaderMessages;

public class HdfDataObjectHeaderPrefixV1 {
    private final int version;                // 1 byte
    private final int totalHeaderMessages;    // 2 bytes
    private final long objectReferenceCount;  // 4 bytes
    private final long objectHeaderSize;      // 4 bytes
    // level 2A1A
    private final List<HdfMessage> dataObjectHeaderMessages;


    // Constructor for application-defined values
    public HdfDataObjectHeaderPrefixV1(int version, int totalHeaderMessages, long objectReferenceCount, long objectHeaderSize, List<HdfMessage> dataObjectHeaderMessages) {
        this.version = version;
        this.totalHeaderMessages = totalHeaderMessages;
        this.objectReferenceCount = objectReferenceCount;
        this.objectHeaderSize = objectHeaderSize;
        this.dataObjectHeaderMessages = dataObjectHeaderMessages;
    }

    // Factory method to read from a FileChannel
    public static HdfDataObjectHeaderPrefixV1 readFromFileChannel(FileChannel fileChannel, int offsetSize, int lengthSize) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN); // Buffer for the fixed-size header
        fileChannel.read(buffer);
        buffer.flip();

        // Parse Version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());

        // Reserved (1 byte, should be zero)
        byte reserved = buffer.get();
        if (reserved != 0) {
            throw new IllegalArgumentException("Reserved byte in Data Object Header Prefix is not zero.");
        }

        // Total Number of Header Messages (2 bytes, little-endian)
        int totalHeaderMessages = Short.toUnsignedInt(buffer.getShort());

        // Object Reference Count (4 bytes, little-endian)
        long objectReferenceCount = Integer.toUnsignedLong(buffer.getInt());

        // Object Header Size (4 bytes, little-endian)
        long objectHeaderSize = Integer.toUnsignedLong(buffer.getInt());

        // Reserved (4 bytes, should be zero)
        int reservedInt = buffer.getInt();
        if (reservedInt != 0) {
            throw new IllegalArgumentException("Reserved integer in Data Object Header Prefix is not zero.");
        }
        List<HdfMessage> dataObjectHeaderMessages = new ArrayList<>();
        parseDataObjectHeaderMessages(fileChannel, (int)objectHeaderSize, offsetSize, lengthSize, dataObjectHeaderMessages);
        for ( HdfMessage hdfMesage: dataObjectHeaderMessages) {
            if (hdfMesage instanceof ContinuationMessage) {
                parseContinuationMessage(fileChannel, (ContinuationMessage)hdfMesage, offsetSize, lengthSize, dataObjectHeaderMessages);
                break;
            }
        }

        // Create the instance
        HdfDataObjectHeaderPrefixV1 prefix = new HdfDataObjectHeaderPrefixV1(version, totalHeaderMessages, objectReferenceCount, objectHeaderSize, dataObjectHeaderMessages);
        return prefix;
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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("HdfDataObjectHeaderPrefixV1 {")
                .append(" Version: ").append(version)
                .append(" Total Header Messages: ").append(totalHeaderMessages)
                .append(" Object Reference Count: ").append(objectReferenceCount)
                .append(" Object Header Size: ").append(objectHeaderSize);
                // Parse header messages
        dataObjectHeaderMessages.forEach(hm->builder.append("\t" + hm));
        builder.append("}");

        return builder.toString();
    }

    public List<HdfMessage> getDataObjectHeaderMessages() {
        return dataObjectHeaderMessages;
    }
}
