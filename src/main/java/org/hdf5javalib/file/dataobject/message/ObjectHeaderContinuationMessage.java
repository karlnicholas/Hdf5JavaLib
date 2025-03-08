package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import lombok.Setter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
@Setter
public class ObjectHeaderContinuationMessage extends HdfMessage {
    private HdfFixedPoint<BigInteger> continuationOffset; // Offset of the continuation block
    private HdfFixedPoint<BigInteger> continuationSize;   // Size of the continuation block

    public ObjectHeaderContinuationMessage(final HdfFixedPoint<BigInteger> continuationOffset, final HdfFixedPoint<BigInteger> continuationSize) {
        super(MessageType.ObjectHeaderContinuationMessage, ()-> (short) (8+8), (byte)0);
        this.continuationOffset = continuationOffset;
        this.continuationSize = continuationSize;
    }

    public static ObjectHeaderContinuationMessage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse the continuation offset and size
        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint<BigInteger> continuationOffset = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, emptyBitSet, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint<BigInteger> continuationSize = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, lengthSize, emptyBitSet, (short) 0, (short)(lengthSize*8));
        return new ObjectHeaderContinuationMessage(continuationOffset, continuationSize);
    }

    @Override
    public String toString() {
        return "ObjectHeaderContinuationMessage{" +
                "continuationOffset=" + continuationOffset +
                ", continuationSize=" + continuationSize +
                '}';
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write B-tree address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, continuationOffset);

        // Write Local Heap address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, continuationSize);
    }
}
