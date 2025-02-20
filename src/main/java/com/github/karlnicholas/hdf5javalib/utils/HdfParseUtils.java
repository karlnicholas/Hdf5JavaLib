package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.datatype.*;
import com.github.karlnicholas.hdf5javalib.message.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

public class HdfParseUtils {
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
            headerMessages.add(createMessageInstance(type, flags, messageData, offsetSize, lengthSize, ()-> Arrays.copyOfRange(messageData, 8, messageData.length)));

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

    private static String readNullTerminatedString(ByteBuffer buffer) {
        StringBuilder nameBuilder = new StringBuilder();
        byte b;
        while ((b = buffer.get()) != 0) {
            nameBuilder.append((char) b);
        }
        return nameBuilder.toString();
    }

    public static HdfDataTypeMember parseMember(ByteBuffer buffer) {
        buffer.mark();
        String name = readNullTerminatedString(buffer);

        // Align to 8-byte boundary
        alignBufferTo8ByteBoundary(buffer, name.length() + 1);

        int offset = buffer.getInt();
        int dimensionality = Byte.toUnsignedInt(buffer.get());
        buffer.position(buffer.position() + 3); // Skip reserved bytes
        int dimensionPermutation = buffer.getInt();
        buffer.position(buffer.position() + 4); // Skip reserved bytes

        int[] dimensionSizes = new int[4];
        for (int j = 0; j < 4; j++) {
            dimensionSizes[j] = buffer.getInt();
        }

        HdfDataType type = parseMemberDataType(buffer, name);
        return new HdfDataTypeMember(name, offset, dimensionality, dimensionPermutation, dimensionSizes, type);

    }

    private static void alignBufferTo8ByteBoundary(ByteBuffer buffer, int dataLength) {
        int padding = (8 - (dataLength % 8)) % 8;
        buffer.position(buffer.position() + padding);
    }

    public static HdfDataType parseMemberDataType(ByteBuffer buffer, String name) {
        byte classAndVersion = buffer.get();
        byte version = (byte) ((classAndVersion >> 4) & 0x0F);
        int dataTypeClass = classAndVersion & 0x0F;

        byte[] classBits = new byte[3];
        buffer.get(classBits);
        BitSet classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        short size = (short) Integer.toUnsignedLong(buffer.getInt());

        return switch (dataTypeClass) {
            case 0 -> parseFixedPoint(buffer, version, size, classBitField, name);
            case 1 -> parseFloatingPoint(buffer, version, size, classBitField, name);
            case 3 -> parseString(version, size, classBitField, name);
            case 6 -> parseCompoundDataType(version, size, classBitField, name, buffer);
            default -> throw new UnsupportedOperationException("Unsupported datatype class: " + dataTypeClass);
        };
    }

    private static CompoundDataType parseCompoundDataType(byte version, short size, BitSet classBitField, String name, ByteBuffer buffer) {
        return new CompoundDataType(classBitField, size, buffer.array());
    }
    private static FixedPointMember parseFixedPoint(ByteBuffer buffer, byte version, short size, BitSet classBitField, String name) {
        boolean bigEndian = classBitField.get(0);
        boolean loPad = classBitField.get(1);
        boolean hiPad = classBitField.get(2);
        boolean signed = classBitField.get(3);

        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();

        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        short messageDataSize = (short) (name.length()+1 + padding + 44);

        return new FixedPointMember(version, size, bigEndian, loPad, hiPad, signed, bitOffset, bitPrecision, messageDataSize, classBitField);
    }

    private static FloatingPointMember parseFloatingPoint(ByteBuffer buffer, byte version, short size, BitSet classBitField, String name) {
        boolean bigEndian = classBitField.get(0);
        int exponentBits = buffer.getInt();
        int mantissaBits = buffer.getInt();
        return new FloatingPointMember(version, size, exponentBits, mantissaBits, bigEndian);
    }

    private static StringMember parseString(byte version, short size, BitSet classBitField, String name) {
        int paddingType = extractBits(classBitField, 0, 3);
        int charSet = extractBits(classBitField, 4, 7);

        String paddingDescription = switch (paddingType) {
            case 0 -> "Null Terminate";
            case 1 -> "Null Pad";
            case 2 -> "Space Pad";
            default -> "Reserved";
        };

        String charSetDescription = switch (charSet) {
            case 0 -> "ASCII";
            case 1 -> "UTF-8";
            default -> "Reserved";
        };

        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        short messageDataSize = (short) (name.length()+1 + padding + 40);

        return new StringMember(version, size, paddingType, paddingDescription, charSet, charSetDescription, messageDataSize);
    }

    private static int extractBits(BitSet bitSet, int start, int end) {
        int value = 0;
        for (int i = start; i <= end; i++) {
            if (bitSet.get(i)) {
                value |= (1 << (i - start));
            }
        }
        return value;
    }
}
