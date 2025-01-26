package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.*;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.message.ContinuationMessage;
import com.github.karlnicholas.hdf5javalib.HdfDataObjectHeader;
import com.github.karlnicholas.hdf5javalib.message.HdfMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteOrder;
import java.util.List;

public class HdfUtils {
    public static int readIntFromFileChannel(FileChannel fileChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // Assume little-endian as per HDF5 spec
        fileChannel.read(buffer);
        buffer.flip();
        return buffer.getInt();
    }

    public static void skipBytes(FileChannel fileChannel, int bytesToSkip) throws IOException {
        fileChannel.position(fileChannel.position() + bytesToSkip);
    }

    // Parse header messages
    public static void parseDataObjectHeaderMessages(FileChannel fileChannel, int objectHeaderSize, int offsetSize, int lengthSize, List<HdfMessage> headerMessages) throws IOException {
        ByteBuffer buffer  = ByteBuffer.allocate(objectHeaderSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();


        while (buffer.hasRemaining()) {
            // Header Message Type (2 bytes, little-endian)
            int type = Short.toUnsignedInt(buffer.getShort());
            int size = Short.toUnsignedInt(buffer.getShort());
            byte flags = buffer.get();
            buffer.position(buffer.position() + 3); // Skip 3 reserved bytes

            // Header Message Data
            byte[] messageData = new byte[size];
            buffer.get(messageData);

            // Add the message to the list
            headerMessages.add(new HdfDataObjectHeader(type, size, flags, messageData, offsetSize, lengthSize).getParsedMessage());

        }
    }

    // TODO: fix recursion
    public static void parseContinuationMessage(FileChannel fileChannel, ContinuationMessage continuationMessage, int offsetSize, int lengthSize, List<HdfMessage> headerMessages) throws IOException {

        long continuationOffset = continuationMessage.getContinuationOffset().getBigIntegerValue().longValue();
        int continuationSize = continuationMessage.getContinuationSize().getBigIntegerValue().intValue();

        // Move to the continuation block offset
        fileChannel.position(continuationOffset);

        // Parse the continuation block messages
        parseDataObjectHeaderMessages(fileChannel, continuationSize, offsetSize, lengthSize, headerMessages);
    }
    public static BtreeV1GroupNode parseBTreeAndLocalHeap(HdfBTreeV1 bTreeNode, HdfLocalHeapContents localHeap) {
        if (bTreeNode.getNodeType() != 0 || bTreeNode.getNodeLevel() != 0) {
            throw new UnsupportedOperationException("Only nodeType=0 and nodeLevel=0 are supported.");
        }

        List<HdfFixedPoint> keys = bTreeNode.getKeys();
        List<HdfFixedPoint> children = bTreeNode.getChildPointers();

        // Validate that the number of keys and children match the B-tree structure
        if (keys.size() != children.size() + 1) {
            throw new IllegalStateException("Invalid B-tree structure: keys and children count mismatch.");
        }

        HdfString objectName = null;
        HdfFixedPoint childAddress = null;
        // Parse each key and corresponding child
        for (int i = 0; i < keys.size(); i++) {
            HdfFixedPoint keyOffset = keys.get(i);
            objectName = localHeap.parseStringAtOffset(keyOffset);

            if (i < children.size()) {
                childAddress = children.get(i);
            }
        }
        return new BtreeV1GroupNode(objectName, childAddress);
    }

}
