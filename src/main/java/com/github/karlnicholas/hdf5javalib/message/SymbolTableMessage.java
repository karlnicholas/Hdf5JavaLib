package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SymbolTableMessage implements HdfMessage {
    private HdfFixedPoint bTreeAddress;
    private HdfFixedPoint localHeapAddress;

    /**
     *
     * @param flags flags
     * @param data data
     * @param offsetSize offsetSize
     * @param lengthSize lengthSize
     * @return {@link HdfMessage}
     */
    @Override
    public HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        return this;
    }


    public HdfFixedPoint getBTreeAddress() {
        return bTreeAddress;
    }

    public HdfFixedPoint getLocalHeapAddress() {
        return localHeapAddress;
    }

    @Override
    public String toString() {
        return "SymbolTableMessage{" +
                "bTreeAddress=" + bTreeAddress.getBigIntegerValue() +
                ", localHeapAddress=" + localHeapAddress.getBigIntegerValue() +
                '}';
    }
}
