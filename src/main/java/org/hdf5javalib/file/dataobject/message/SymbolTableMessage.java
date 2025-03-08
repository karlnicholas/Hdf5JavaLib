package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class SymbolTableMessage extends HdfMessage {
    private final HdfFixedPoint<BigInteger> bTreeAddress;
    private final HdfFixedPoint<BigInteger> localHeapAddress;

    // Constructor to create SymbolTableMessage directly with values
    public SymbolTableMessage(HdfFixedPoint<BigInteger> bTreeAddress, HdfFixedPoint<BigInteger> localHeapAddress) {
        super(MessageType.SymbolTableMessage, ()-> (short) (bTreeAddress.getSizeMessageData() + localHeapAddress.getSizeMessageData()), (byte)0);
        this.bTreeAddress = bTreeAddress;
        this.localHeapAddress = localHeapAddress;
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        HdfFixedPoint<BigInteger> bTreeAddress = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
        HdfFixedPoint<BigInteger> localHeapAddress = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
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
                "bTreeAddress=" + bTreeAddress.getInstance() +
                ", localHeapAddress=" + localHeapAddress.getInstance() +
                '}';
    }
}
