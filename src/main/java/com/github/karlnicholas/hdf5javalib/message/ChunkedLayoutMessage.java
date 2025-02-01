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

    public ChunkedLayoutMessage(int version, int rank, long[] chunkSizes, HdfFixedPoint address) {
        this.version = version;
        this.rank = rank;
        this.chunkSizes = chunkSizes;
        this.address = address;
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int version = Byte.toUnsignedInt(buffer.get());
        int rank = Byte.toUnsignedInt(buffer.get());
        long[] chunkSizes = new long[rank];
        for (int i = 0; i < rank; i++) {
            chunkSizes[i] = Integer.toUnsignedLong(buffer.getInt());
        }
        HdfFixedPoint address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        return new ChunkedLayoutMessage(version, rank, chunkSizes, address);
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

    @Override
    public void writeToByteBuffer(ByteBuffer buffer, int offsetSize) {

    }
}
