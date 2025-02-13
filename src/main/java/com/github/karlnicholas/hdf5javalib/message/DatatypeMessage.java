package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
public class DatatypeMessage extends HdfMessage {
    private final int version;                 // Version of the datatype message
    private final int dataTypeClass;           // Datatype class
    private final BitSet classBitField;        // Class Bit Field (24 bits)
    private final HdfFixedPoint size;          // Size of the datatype element
    private final HdfDataType hdfDataType;                 // Remaining raw data

    // Constructor to initialize all fields
    public DatatypeMessage(int version, int dataTypeClass, BitSet classBitField, HdfFixedPoint size, HdfDataType hdfDataType) {
        super(MessageType.DatatypeMessage, ()-> {
            short sizeMessageData = 8;
            sizeMessageData += hdfDataType.getSizeMessageData();
            // to 8 byte boundary
            return (short) ((sizeMessageData + 7) & ~7);
        },(byte)1);
        this.version = version;
        this.dataTypeClass = dataTypeClass;
        this.classBitField = classBitField;
        this.size = size;
        this.hdfDataType = hdfDataType;
    }

    /**
     * Parses the header message and returns a constructed instance.
     *
     * @param flags      Flags associated with the message (not used here).
     * @param data       Byte array containing the header message data.
     * @param offsetSize Size of offsets in bytes (not used here).
     * @param lengthSize Size of lengths in bytes (not used here).
     * @return A fully constructed `DatatypeMessage` instance.
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize, byte[] dataTypeData) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse Version and Datatype Class (packed into a single byte)
        byte classAndVersion = buffer.get();
        int version = (classAndVersion >> 4) & 0x0F; // Top 4 bits
        int dataTypeClass = classAndVersion & 0x0F;  // Bottom 4 bits

        // Parse Class Bit Field (24 bits)
        byte[] classBits = new byte[3];
        buffer.get(classBits);
        BitSet classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        // Parse Size (unsigned 4 bytes)
        HdfFixedPoint size = HdfFixedPoint.readFromByteBuffer(buffer, (short) 4, false);
        // Return a constructed instance of DatatypeMessage
        HdfDataType hdfDataType;
        if ( dataTypeClass == 6) {
            hdfDataType = new CompoundDataType(classBitField, size.getBigIntegerValue().intValue(), dataTypeData);
        } else if ( dataTypeClass == 3) {
//            byte[] dataBytes = new byte[size];
//            buffer.get(dataBytes);
//            dt.addDataType(dataBytes);
//            value = dt.getHdfDataType();
            // Return a constructed instance of DatatypeMessage
            hdfDataType = new HdfString(dataTypeData, false, false);
        } else {
            throw new IllegalStateException("Unsupported data type class: " + dataTypeClass);
        }

        return new DatatypeMessage(version, dataTypeClass, classBitField, size, hdfDataType);
    }

//    public void addDataType(byte[] remainingData) {
//        if ( dataTypeClass == 6) {
//            hdfDataType = new CompoundDataType(classBitField, size.getBigIntegerValue().intValue(), remainingData);
//        } else if ( dataTypeClass == 3) {
//            // Return a constructed instance of DatatypeMessage
//            hdfDataType = new HdfString(remainingData, false, false);
//        } else {
//            throw new IllegalStateException("Unsupported data type class: " + dataTypeClass);
//        }
//    }

//    public void setDataType(HdfDataType hdfDataType) {
//        this.hdfDataType = hdfDataType;
//    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DatatypeMessage{");
        sb.append("version=").append(version);
        sb.append(", dataTypeClass=").append(dataTypeClass).append(" (").append(dataTypeClassToString(dataTypeClass)).append(")");
        sb.append(", classBitField=").append(bitSetToString(classBitField, 24));
        sb.append(", size=").append(size.getBigIntegerValue());
        sb.append(", hdfDataType=").append(hdfDataType);
        sb.append('}');
        return sb.toString();
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
        byte classAndVersion = (byte) ((version << 4) + dataTypeClass);
        buffer.put(classAndVersion);
        // Parse Class Bit Field (24 bits)
        byte[] bytes = classBitField.toByteArray();
        byte[] result = new byte[3];
        // Fill with 0s to ensure exactly 3 bytes
        Arrays.fill(result, (byte) 0);
        // Copy bytes in little-endian order
        System.arraycopy(bytes, 0, result, 0, Math.min(bytes.length, 3));
        buffer.put(result);
        writeFixedPointToBuffer(buffer, size);
        hdfDataType.writeDefinitionToByteBuffer(buffer);
    }
}
