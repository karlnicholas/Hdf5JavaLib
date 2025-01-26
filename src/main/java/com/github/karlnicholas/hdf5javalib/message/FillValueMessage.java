package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class FillValueMessage implements HdfMessage {
    private int version;              // 1 byte
    private int spaceAllocationTime;  // 1 byte
    private int fillValueWriteTime;   // 1 byte
    private int fillValueDefined;     // 1 byte
    private HdfFixedPoint size;       // Size of the Fill Value field (optional, unsigned 4 bytes)
    private byte[] fillValue;         // Fill Value field (optional)

    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse the first 4 bytes
        this.version = Byte.toUnsignedInt(buffer.get());
        this.spaceAllocationTime = Byte.toUnsignedInt(buffer.get());
        this.fillValueWriteTime = Byte.toUnsignedInt(buffer.get());
        this.fillValueDefined = Byte.toUnsignedInt(buffer.get());

        // Handle Version 2 behavior
        if (version >= 2 && fillValueDefined == 1) {
            // Parse Size (unsigned 4 bytes, using HdfFixedPoint)
            this.size = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);

            // Parse Fill Value
            int sizeValue = size.getBigIntegerValue().intValue();
            this.fillValue = new byte[sizeValue];
            buffer.get(this.fillValue);
        } else {
            this.size = null;      // No Size field if Fill Value Defined is 0
            this.fillValue = null; // No Fill Value if Fill Value Defined is 0
        }

        return this;
    }

    @Override
    public String toString() {
        return "FillValueMessage{" +
                "version=" + version +
                ", spaceAllocationTime=" + spaceAllocationTime +
                ", fillValueWriteTime=" + fillValueWriteTime +
                ", fillValueDefined=" + fillValueDefined +
                ", size=" + (size != null ? size.getBigIntegerValue() : "undefined") +
                ", fillValue=" + (fillValue != null ? fillValue.length + " bytes" : "undefined") +
                '}';
    }

    // Getters
    public int getVersion() {
        return version;
    }

    public int getSpaceAllocationTime() {
        return spaceAllocationTime;
    }

    public int getFillValueWriteTime() {
        return fillValueWriteTime;
    }

    public int getFillValueDefined() {
        return fillValueDefined;
    }

    public HdfFixedPoint getSize() {
        return size;
    }

    public byte[] getFillValue() {
        return fillValue;
    }
}
