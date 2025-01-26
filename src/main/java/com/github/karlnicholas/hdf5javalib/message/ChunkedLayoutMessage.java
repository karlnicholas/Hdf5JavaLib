package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class ChunkedLayoutMessage implements HdfMessage {
    private int version;
    private int rank;
    private long[] chunkSizes;
    private HdfFixedPoint address;

    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        this.version = Byte.toUnsignedInt(buffer.get());
        this.rank = Byte.toUnsignedInt(buffer.get());
        this.chunkSizes = new long[rank];
        for (int i = 0; i < rank; i++) {
            this.chunkSizes[i] = Integer.toUnsignedLong(buffer.getInt());
        }
        this.address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        return this;
    }

    @Override
    public String toString() {
        return "ChunkedLayoutMessage{" +
                "version=" + version +
                ", rank=" + rank +
                ", chunkSizes=" + Arrays.toString(chunkSizes) +
                ", address=" + (address != null ? address.getBigIntegerValue() : "null") +
                '}';
    }
}
