package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class HdfBTreeV1 {
    private final String signature;
    private final int nodeType;
    private final int nodeLevel;
    private final int entriesUsed;
    private final HdfFixedPoint leftSiblingAddress;
    private final HdfFixedPoint rightSiblingAddress;
    private final List<HdfFixedPoint> childPointers;
    private final List<HdfFixedPoint> keys;

    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            List<HdfFixedPoint> childPointers,
            List<HdfFixedPoint> keys
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.childPointers = childPointers;
        this.keys = keys;
    }

    public static HdfBTreeV1 readFromFileChannel(FileChannel fileChannel, long position, int offsetSize, int lengthSize) throws IOException {
        // Prepare a buffer for the initial read
        ByteBuffer buffer = ByteBuffer.allocate(24).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        fileChannel.position(position);
        fileChannel.read(buffer);
        buffer.flip();
        // Read and verify the signature
        byte[] signatureBytes = new byte[4];
        buffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"TREE".equals(signature)) {
            throw new IllegalArgumentException("Invalid B-tree node signature: " + signature);
        }

        // Read metadata fields
        int nodeType = Byte.toUnsignedInt(buffer.get());
        if (nodeType != 0) {
            throw new UnsupportedOperationException("Node type " + nodeType + " is not supported.");
        }

        int nodeLevel = Byte.toUnsignedInt(buffer.get());
        int entriesUsed = Short.toUnsignedInt(buffer.getShort());

        // Read sibling addresses
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);

        // Allocate buffer for keys and child pointers
        int keyPointerBufferSize = (entriesUsed * (lengthSize + offsetSize)) + lengthSize;
        buffer = ByteBuffer.allocate(keyPointerBufferSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        // Read keys and child pointers
        fileChannel.read(buffer);
        buffer.flip();
        List<HdfFixedPoint> keys = new ArrayList<>();
        List<HdfFixedPoint> childPointers = new ArrayList<>();

        for (int i = 0; i < entriesUsed; i++) {
            keys.add(HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false));
            childPointers.add(HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false));
        }

        // Read the final key
        keys.add(HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false));

        return new HdfBTreeV1(
                signature,
                nodeType,
                nodeLevel,
                entriesUsed,
                leftSiblingAddress,
                rightSiblingAddress,
                childPointers,
                keys
        );
    }

    public int getNodeType() {
        return nodeType;
    }

    public int getNodeLevel() {
        return nodeLevel;
    }

    public int getEntriesUsed() {
        return entriesUsed;
    }

    public List<HdfFixedPoint> getChildPointers() {
        return childPointers;
    }

    public List<HdfFixedPoint> getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "HdfBTreeV1{" +
                "signature='" + signature + '\'' +
                ", nodeType=" + nodeType +
                ", nodeLevel=" + nodeLevel +
                ", entriesUsed=" + entriesUsed +
                ", leftSiblingAddress=" + leftSiblingAddress +
                ", rightSiblingAddress=" + rightSiblingAddress +
                ", childPointers=" + childPointers +
                ", keys=" + keys +
                '}';
    }
}
