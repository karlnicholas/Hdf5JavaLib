package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

public class DataTypeMessage implements HdfMessage {
    private final int version;                 // Version of the datatype message
    private final int dataTypeClass;           // Datatype class
    private final BitSet classBitField;        // Class Bit Field (24 bits)
    private final HdfFixedPoint size;          // Size of the datatype element
    private final byte[] data;                 // Remaining raw data

    // Constructor to initialize all fields
    public DataTypeMessage(int version, int dataTypeClass, BitSet classBitField, HdfFixedPoint size, byte[] data) {
        this.version = version;
        this.dataTypeClass = dataTypeClass;
        this.classBitField = classBitField;
        this.size = size;
        this.data = data;
    }

    /**
     * Parses the header message and returns a constructed instance.
     *
     * @param flags      Flags associated with the message (not used here).
     * @param data       Byte array containing the header message data.
     * @param offsetSize Size of offsets in bytes (not used here).
     * @param lengthSize Size of lengths in bytes (not used here).
     * @return A fully constructed `DataTypeMessage` instance.
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
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
        HdfFixedPoint size = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);

        // Extract remaining raw data
        byte[] remainingData = Arrays.copyOfRange(data, buffer.position(), buffer.limit());

        // Return a constructed instance of DataTypeMessage
        return new DataTypeMessage(version, dataTypeClass, classBitField, size, remainingData);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("DataTypeMessage{");
        sb.append("version=").append(version);
        sb.append(", dataTypeClass=").append(dataTypeClass).append(" (").append(dataTypeClassToString(dataTypeClass)).append(")");
        sb.append(", classBitField=").append(bitSetToString(classBitField, 24));
        sb.append(", size=").append(size.getBigIntegerValue());
        sb.append(", data.length=").append(data.length);
        sb.append('}');
        return sb.toString();
    }

    // Helper method to convert the dataTypeClass to a human-readable string
    private String dataTypeClassToString(int dataTypeClass) {
        switch (dataTypeClass) {
            case 0: return "Fixed-Point";
            case 1: return "Floating-Point";
            case 2: return "Time";
            case 3: return "String";
            case 4: return "Bit Field";
            case 5: return "Opaque";
            case 6: return "Compound";
            case 7: return "Reference";
            case 8: return "Enumerated";
            case 9: return "Variable-Length";
            case 10: return "Array";
            default: return "Unknown";
        }
    }

    // Helper method to convert a BitSet to a binary string
    private String bitSetToString(BitSet bitSet, int numBits) {
        StringBuilder bits = new StringBuilder();
        for (int i = numBits - 1; i >= 0; i--) {
            bits.append(bitSet.get(i) ? "1" : "0");
        }
        return bits.toString();
    }

    // Getters for all fields
    public int getVersion() {
        return version;
    }

    public int getDataTypeClass() {
        return dataTypeClass;
    }

    public BitSet getClassBitField() {
        return classBitField;
    }

    public HdfFixedPoint getSize() {
        return size;
    }

    public byte[] getData() {
        return data;
    }
}
