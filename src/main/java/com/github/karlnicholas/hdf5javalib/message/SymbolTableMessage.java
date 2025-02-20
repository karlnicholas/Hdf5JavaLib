package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
public class SymbolTableMessage extends HdfMessage {
    private final HdfFixedPoint bTreeAddress;
    private final HdfFixedPoint localHeapAddress;

    // Constructor to create SymbolTableMessage directly with values
    public SymbolTableMessage(HdfFixedPoint bTreeAddress, HdfFixedPoint localHeapAddress) {
        super(MessageType.SymbolTableMessage, ()-> (short) (bTreeAddress.getSizeMessageData() + localHeapAddress.getSizeMessageData()), (byte)0);
        this.bTreeAddress = bTreeAddress;
        this.localHeapAddress = localHeapAddress;
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        HdfFixedPoint bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        HdfFixedPoint localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        return new SymbolTableMessage(bTreeAddress, localHeapAddress);
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write B-tree address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, bTreeAddress);

        // Write Local Heap address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, localHeapAddress);
    }

    @Override
    public String toString() {
        return "SymbolTableMessage{" +
                "bTreeAddress=" + bTreeAddress.getBigIntegerValue() +
                ", localHeapAddress=" + localHeapAddress.getBigIntegerValue() +
                '}';
    }
}
