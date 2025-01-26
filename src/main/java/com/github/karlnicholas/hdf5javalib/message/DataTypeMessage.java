package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.HdfDataObjectHeader;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

public class DataTypeMessage implements HdfMessage {
    private int version;                 // Version of the datatype message

    private int dataTypeClass;           // Datatype class
    private BitSet classBitField;        // Class Bit Field (24 bits)
    private HdfFixedPoint size;          // Size of the datatype element
    private byte[] data;

    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse Version and Datatype Class (packed into a single byte)
        byte classAndVersion = buffer.get();
        this.version = (classAndVersion >> 4) & 0x0F; // Top 4 bits
        this.dataTypeClass = classAndVersion & 0x0F;  // Bottom 4 bits

        // Parse Class Bit Field (24 bits)
        byte[] classBits = new byte[3];
        buffer.get(classBits);
        this.classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        // Parse Size (unsigned 4 bytes)
        this.size = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);
        this.data = Arrays.copyOfRange(data, buffer.position(), buffer.limit());

        return this;
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

    private String bitSetToString(BitSet bitSet, int numBits) {
        StringBuilder bits = new StringBuilder();
        for (int i = numBits - 1; i >= 0; i--) {
            bits.append(bitSet.get(i) ? "1" : "0");
        }
        return bits.toString();
    }
}
