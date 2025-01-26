package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ContiguousLayoutMessage implements HdfMessage {
    private int version;
    private HdfFixedPoint address;
    private HdfFixedPoint size;

    public ContiguousLayoutMessage(int version, HdfFixedPoint address, HdfFixedPoint size) {
        this.version = version;
        this.address = address;
        this.size = size;
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int version = Byte.toUnsignedInt(buffer.get());
        HdfFixedPoint address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        HdfFixedPoint size = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
        return new ContiguousLayoutMessage(version, address, size);
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
