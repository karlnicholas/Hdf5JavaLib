package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HdfSymbolTableMessage implements HdfMessage {
    private HdfFixedPoint bTreeAddress;
    private HdfFixedPoint localHeapAddress;

    /**
     * Parses a Symbol Table Message from a ByteBuffer.
     *
     * @param buffer     The ByteBuffer containing the Symbol Table Message data.
     * @param offsetSize The size of offsets specified in the superblock (in bytes).
     * @return A parsed HdfSymbolTableMessage instance.
     */
//    public static HdfSymbolTableMessage fromByteBuffer(ByteBuffer buffer, int offsetSize) {
//        HdfFixedPoint bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
//        HdfFixedPoint localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
//        return new HdfSymbolTableMessage(bTreeAddress, localHeapAddress);
//    }
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
        return "HdfSymbolTableMessage{" +
                "bTreeAddress=" + bTreeAddress.getBigIntegerValue() +
                ", localHeapAddress=" + localHeapAddress.getBigIntegerValue() +
                '}';
    }
}
