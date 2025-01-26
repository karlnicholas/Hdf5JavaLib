package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.*;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.HdfDataObjectHeader;

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
            headerMessages.add(createMessageInstance(type, flags, messageData, offsetSize, lengthSize));

        }
    }

    public static HdfMessage createMessageInstance(int type, byte flags, byte[] data, int offsetSize, int lengthSize) {
        switch (type) {
            case 0:
                return new NullMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 1:
                return new DataSpaceMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 3:
                return new DataTypeMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 5:
                return new FillValueMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 8:
                return new DataLayoutMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 12:
                return new AttributeMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 18:
                return new ObjectModificationTimeMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 16:
                return new ContinuationMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 17:
                return new SymbolTableMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 19:
                return new BTreeKValuesMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            default:
                throw new IllegalArgumentException("Unknown message type: " + type);
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
