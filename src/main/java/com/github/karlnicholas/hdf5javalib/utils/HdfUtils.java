package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.message.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

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
    public static void parseDataObjectHeaderMessages(FileChannel fileChannel, short objectHeaderSize, short offsetSize, short lengthSize, List<HdfMessage> headerMessages) throws IOException {
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
            headerMessages.add(createMessageInstance(type, flags, messageData, offsetSize, lengthSize, ()->Arrays.copyOfRange(messageData, 8, messageData.length)));

        }
    }

    public static HdfMessage createMessageInstance(int type, byte flags, byte[] data, short offsetSize, short lengthSize, Supplier<byte[]> getDataTypeData) {
        return switch (type) {
            case 0 -> NullMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 1 -> DataSpaceMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 3 -> DataTypeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize, getDataTypeData.get());
//            {
//                DataTypeMessage dataTypeMessage = (DataTypeMessage)DataTypeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
//                dataTypeMessage.addDataType(Arrays.copyOfRange(data, 8, data.length));
//                yield dataTypeMessage;
//            }
            case 5 -> FillValueMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 8 -> DataLayoutMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 12 -> AttributeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 16 -> ContinuationMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 17 -> SymbolTableMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 18 -> ObjectModificationTimeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 19 -> BTreeKValuesMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        };
    }

    // TODO: fix recursion
    public static void parseContinuationMessage(FileChannel fileChannel, ContinuationMessage continuationMessage, short offsetSize, short lengthSize, List<HdfMessage> headerMessages) throws IOException {

        long continuationOffset = continuationMessage.getContinuationOffset().getBigIntegerValue().longValue();
        short continuationSize = continuationMessage.getContinuationSize().getBigIntegerValue().shortValueExact();

        // Move to the continuation block offset
        fileChannel.position(continuationOffset);

        // Parse the continuation block messages
        parseDataObjectHeaderMessages(fileChannel, continuationSize, offsetSize, lengthSize, headerMessages);
    }

    public static void printData(FileChannel fileChannel, CompoundDataType compoundDataType, long dataAddress, long dimension ) throws IOException {
        Object[] data = new Object[17];
        fileChannel.position(dataAddress);
        for ( int i=0; i <dimension; ++i) {
            ByteBuffer dataBuffer = ByteBuffer.allocate(compoundDataType.getSize()).order(ByteOrder.LITTLE_ENDIAN);
            fileChannel.read(dataBuffer);
            dataBuffer.flip();
            for ( int column = 0; column < compoundDataType.getNumberOfMembers(); ++column ) {
                CompoundDataType.Member member = compoundDataType.getMembers().get(column);
                dataBuffer.position(member.getOffset());
                if (member.getType() instanceof CompoundDataType.StringMember) {
                    data[column] = ((CompoundDataType.StringMember) member.getType()).getInstance(dataBuffer);
                } else if (member.getType() instanceof CompoundDataType.FixedPointMember) {
                    data[column] = ((CompoundDataType.FixedPointMember) member.getType()).getInstance(dataBuffer);
                } else if (member.getType() instanceof CompoundDataType.FloatingPointMember) {
                    data[column] = ((CompoundDataType.FloatingPointMember) member.getType()).getInstance();
                } else {
                    throw new UnsupportedOperationException("Member type " + member.getType() + " not yet implemented.");
                }
            }
            System.out.println(Arrays.toString(data));
        }

    }
    /**
     * Writes an `HdfFixedPoint` value to the `ByteBuffer` in **little-endian format**.
     * If undefined, fills with 0xFF.
     */
    public static void writeFixedPointToBuffer(ByteBuffer buffer, HdfFixedPoint value) {
        short size = value.getSizeMessageData();
        byte[] bytesToWrite = new byte[size];

        if (value.isUndefined()) {
            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
        } else {
            byte[] valueBytes = value.getBigIntegerValue().toByteArray();
            int copySize = Math.min(valueBytes.length, size);

            // Store in **little-endian format** by reversing byte order
            for (int i = 0; i < copySize; i++) {
                bytesToWrite[i] = valueBytes[copySize - 1 - i];
            }
        }

        buffer.put(bytesToWrite);
    }
}
