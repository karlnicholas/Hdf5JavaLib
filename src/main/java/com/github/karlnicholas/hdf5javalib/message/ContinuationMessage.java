package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ContinuationMessage implements HdfMessage {
    private HdfFixedPoint continuationOffset; // Offset of the continuation block
    private HdfFixedPoint continuationSize;   // Size of the continuation block

    public ContinuationMessage(final HdfFixedPoint continuationOffset, final HdfFixedPoint continuationSize) {
        this.continuationOffset = continuationOffset;
        this.continuationSize = continuationSize;
    }

    public static ContinuationMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse the continuation offset and size
        HdfFixedPoint continuationOffset = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        HdfFixedPoint continuationSize = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
        return new ContinuationMessage(continuationOffset, continuationSize);
    }

    public HdfFixedPoint getContinuationOffset() {
        return continuationOffset;
    }

    public HdfFixedPoint getContinuationSize() {
        return continuationSize;
    }

    @Override
    public String toString() {
        return "ContinuationMessage{" +
                "continuationOffset=" + continuationOffset +
                ", continuationSize=" + continuationSize +
                '}';
    }
}
