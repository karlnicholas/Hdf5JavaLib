package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.*;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static com.github.karlnicholas.hdf5javalib.datatype.FixedPointDatatype.parseFixedPointType;
import static com.github.karlnicholas.hdf5javalib.datatype.FloatingPointDatatype.parseFloatingPointType;
import static com.github.karlnicholas.hdf5javalib.datatype.StringDatatype.parseStringType;

@Getter
public class DatatypeMessage extends HdfMessage {
    private final HdfDatatype hdfDatatype;                 // Remaining raw data

    // Constructor to initialize all fields
    public DatatypeMessage(HdfDatatype hdfDatatype) {
        super(MessageType.DatatypeMessage, ()-> {
            short sizeMessageData = 8;
            sizeMessageData += hdfDatatype.getSizeMessageData();
            // to 8 byte boundary
            return (short) ((sizeMessageData + 7) & ~7);
        },(byte)1);
        this.hdfDatatype = hdfDatatype;
    }

//    /**
//     * Parses the header message and returns a constructed instance.
//     *
//     * @param flags      Flags associated with the message (not used here).
//     * @param data       Byte array containing the header message data.
//     * @param offsetSize Size of offsets in bytes (not used here).
//     * @param lengthSize Size of lengths in bytes (not used here).
//     * @return A fully constructed `DatatypeMessage` instance.
//     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize, byte[] dataTypeData) {
//    public static HdfMessage parseHeaderMessage(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse Version and Datatype Class (packed into a single byte)
        byte classAndVersion = buffer.get();
//        byte version = (byte) ((classAndVersion >> 4) & 0x0F); // Top 4 bits
//        byte dataTypeClass = (byte) (classAndVersion & 0x0F);  // Bottom 4 bits

        // Parse Class Bit Field (24 bits)
        byte[] classBits = new byte[3];
        buffer.get(classBits);
        BitSet classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        // Parse Size (unsigned 4 bytes)
        int size = buffer.getInt();
        HdfDatatype hdfDatatype = parseMessageDataType(classAndVersion, classBitField, size, buffer);
        return new DatatypeMessage(hdfDatatype);
    }
//    public static HdfCompoundDatatypeMember parseMember(ByteBuffer buffer) {
//        buffer.mark();
//        String name = readNullTerminatedString(buffer);
//
//        // Align to 8-byte boundary
//        alignBufferTo8ByteBoundary(buffer, name.length() + 1);
//
//        int offset = buffer.getInt();
//        int dimensionality = Byte.toUnsignedInt(buffer.get());
//        buffer.position(buffer.position() + 3); // Skip reserved bytes
//        int dimensionPermutation = buffer.getInt();
//        buffer.position(buffer.position() + 4); // Skip reserved bytes
//
//        int[] dimensionSizes = new int[4];
//        for (int j = 0; j < 4; j++) {
//            dimensionSizes[j] = buffer.getInt();
//        }
//
//        HdfDatatype type = parseMessageDataType(buffer, name);
//        return new HdfCompoundDatatypeMember(name, offset, dimensionality, dimensionPermutation, dimensionSizes, type);
//
//    }

    private static HdfDatatype parseMessageDataType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        int dataTypeClass = classAndVersion & 0x0F;
        return switch (dataTypeClass) {
            case 0 -> parseFixedPointType(classAndVersion, classBitField, size, buffer);
            case 1 -> parseFloatingPointType(classAndVersion, classBitField, size, buffer);
            case 3 -> parseStringType(classAndVersion, classBitField, size, buffer);
            case 6 -> new CompoundDatatype(classAndVersion, classBitField, size, buffer);
            default -> throw new UnsupportedOperationException("Unsupported datatype class: " + dataTypeClass);
        };
    }

    @Override
    public String toString() {
        return "DatatypeMessage{hdfDatatype=" + hdfDatatype + '}';
    }

    // Helper method to convert the dataTypeClass to a human-readable string
    private String dataTypeClassToString(int dataTypeClass) {
        return switch (dataTypeClass) {
            case 0 -> "Fixed-Point";
            case 1 -> "Floating-Point";
            case 2 -> "Time";
            case 3 -> "String";
            case 4 -> "Bit Field";
            case 5 -> "Opaque";
            case 6 -> "Compound";
            case 7 -> "Reference";
            case 8 -> "Enumerated";
            case 9 -> "Variable-Length";
            case 10 -> "Array";
            default -> "Unknown";
        };
    }

    // Helper method to convert a BitSet to a binary string
    private String bitSetToString(BitSet bitSet, int numBits) {
        StringBuilder bits = new StringBuilder();
        for (int i = numBits - 1; i >= 0; i--) {
            bits.append(bitSet.get(i) ? "1" : "0");
        }
        return bits.toString();
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);

//        // needed for compoundtype but duplicate when fixedpoint type
//        // Datatype general information
//        byte classAndVersion = (byte) ((version << 4) + dataTypeClass);
//        buffer.put(classAndVersion);    // 1
//        // Parse Class Bit Field (24 bits)
//        byte[] bytes = classBitField.toByteArray();
//        byte[] result = new byte[3];
//        // Copy bytes
//        System.arraycopy(bytes, 0, result, 0, Math.min(bytes.length, 3));
//        buffer.put(result);         // 3
//        buffer.putInt(size);        // 4
//
        // datatype specifics
        writeInfoToByteBuffer(this, buffer);
    }

    public static void writeInfoToByteBuffer(DatatypeMessage datatypeMessage, ByteBuffer buffer) {
        datatypeMessage.hdfDatatype.writeDefinitionToByteBuffer(buffer);
    }
}
