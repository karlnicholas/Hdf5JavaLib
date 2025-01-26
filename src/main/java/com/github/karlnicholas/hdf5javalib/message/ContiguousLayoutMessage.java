package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ContiguousLayoutMessage implements HdfMessage {
    private int version;
    private HdfFixedPoint address;
    private HdfFixedPoint size;

    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.version = Byte.toUnsignedInt(buffer.get());
        this.address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        this.size = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
        return this;
    }

    @Override
    public String toString() {
        return "ContiguousLayoutMessage{" +
                "version=" + version +
                ", address=" + (address != null ? address.getBigIntegerValue() : "null") +
                ", size=" + (size != null ? size.getBigIntegerValue() : "null") +
                '}';
    }
}
