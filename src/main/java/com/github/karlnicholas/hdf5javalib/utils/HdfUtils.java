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
            MessageType type = MessageType.fromValue(buffer.getShort());
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

    public static HdfMessage createMessageInstance(MessageType type, byte flags, byte[] data, short offsetSize, short lengthSize, Supplier<byte[]> getDataTypeData) {
        return switch (type) {
            case NilMessage -> NilMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case DataspaceMessage -> DataspaceMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case DatatypeMessage -> DatatypeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize, getDataTypeData.get());
//            {
//                DatatypeMessage dataTypeMessage = (DatatypeMessage)DatatypeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
//                dataTypeMessage.addDataType(Arrays.copyOfRange(data, 8, data.length));
//                yield dataTypeMessage;
//            }
            case FillValueMessage -> FillValueMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case DataLayoutMessage -> DataLayoutMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case AttributeMessage -> AttributeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case ObjectHeaderContinuationMessage -> ObjectHeaderContinuationMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case SymbolTableMessage -> SymbolTableMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case ObjectModificationTimeMessage -> ObjectModificationTimeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case BtreeKValuesMessage -> BTreeKValuesMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        };
    }

    // TODO: fix recursion
    public static void parseContinuationMessage(FileChannel fileChannel, ObjectHeaderContinuationMessage objectHeaderContinuationMessage, short offsetSize, short lengthSize, List<HdfMessage> headerMessages) throws IOException {

        long continuationOffset = objectHeaderContinuationMessage.getContinuationOffset().getBigIntegerValue().longValue();
        short continuationSize = objectHeaderContinuationMessage.getContinuationSize().getBigIntegerValue().shortValueExact();

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
    public static void dumpByteBuffer(ByteBuffer buffer) {
        int bytesPerLine = 16; // 16 bytes per row
        int limit = buffer.limit();
        buffer.rewind(); // Reset position to 0 before reading

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < limit; i += bytesPerLine) {
            // Print the address (memory offset in hex)
            sb.append(String.format("%08X:  ", i));

            StringBuilder ascii = new StringBuilder();

            // Print the first 8 bytes (hex values)
            for (int j = 0; j < 8; j++) {
                buildHexValues(buffer, limit, sb, i, ascii, j);
            }

            sb.append(" "); // Space separator

            // Print the second 8 bytes (hex values)
            for (int j = 8; j < bytesPerLine; j++) {
                buildHexValues(buffer, limit, sb, i, ascii, j);
            }

            // Append ASCII representation
            sb.append("  ").append(ascii);

            // Newline for next row
            sb.append("\n");
        }

        System.out.print(sb);
    }

    private static void buildHexValues(ByteBuffer buffer, int limit, StringBuilder sb, int i, StringBuilder ascii, int j) {
        if (i + j < limit) {
            byte b = buffer.get(i + j);
            sb.append(String.format("%02X ", b));
            ascii.append(isPrintable(b) ? (char) b : '.');
        } else {
            sb.append("   "); // Padding for incomplete lines
            ascii.append(" ");
        }
    }

    // Helper method to check if a byte is a printable ASCII character (excluding control chars)
    private static boolean isPrintable(byte b) {
        return (b >= 32 && b <= 126); // Includes extended ASCII
    }

}
