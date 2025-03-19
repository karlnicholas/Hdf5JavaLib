package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class SymbolTableMessage extends HdfMessage {
    private final HdfFixedPoint bTreeAddress;
    private final HdfFixedPoint localHeapAddress;

    // Constructor to create SymbolTableMessage directly with values
    public SymbolTableMessage(HdfFixedPoint bTreeAddress, HdfFixedPoint localHeapAddress) {
        super(MessageType.SymbolTableMessage, ()-> (short) (bTreeAddress.getDatatype().getSize() + localHeapAddress.getDatatype().getSize()), (byte)0);
        this.bTreeAddress = bTreeAddress;
        this.localHeapAddress = localHeapAddress;
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        HdfFixedPoint bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
        HdfFixedPoint localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
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
                "bTreeAddress=" + bTreeAddress.getInstance(Long.class) +
                ", localHeapAddress=" + localHeapAddress.getInstance(Long.class) +
                '}';
    }
}
