package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.utils.BtreeV1GroupNode;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
public class HdfBTreeV1 {
    private final String signature;
    private final int nodeType;
    private final int nodeLevel;
    private final int entriesUsed;
    private final HdfFixedPoint leftSiblingAddress;
    private final HdfFixedPoint rightSiblingAddress;
    private final List<HdfFixedPoint> childPointers;
    private final List<HdfFixedPoint> keys;
    // set later
    private List<BtreeV1GroupNode> groupNodes;

    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            List<HdfFixedPoint> childPointers,
            List<HdfFixedPoint> keys,
            List<BtreeV1GroupNode> groupNodes
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.childPointers = childPointers;
        this.keys = keys;
        this.groupNodes = groupNodes;
    }

    public static HdfBTreeV1 readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize) throws IOException {
        // Prepare a buffer for the initial read
        ByteBuffer buffer = ByteBuffer.allocate(24).order(java.nio.ByteOrder.LITTLE_ENDIAN);
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
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize) ? HdfFixedPoint.undefined(buffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize) ? HdfFixedPoint.undefined(buffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);

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
                keys,
                null
        );
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
        // Step 1: Write the "TREE" signature (4 bytes)
        buffer.put(signature.getBytes());

        // Step 2: Write Node Type (1 byte)
        buffer.put((byte) nodeType);

        // Step 3: Write Node Level (1 byte)
        buffer.put((byte) nodeLevel);

        // Step 4: Write Entries Used (2 bytes, little-endian)
        buffer.putShort((short) entriesUsed);

        // Step 5: Write Left Sibling Address (offsetSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, leftSiblingAddress);

        // Step 6: Write Right Sibling Address (offsetSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, rightSiblingAddress);


        if (nodeType == 0) {
            writeFixedPointToBuffer(buffer, HdfFixedPoint.of(0));
            writeFixedPointToBuffer(buffer, childPointers.get(0));

            // Interleave Keys and Child Pointers
            for (int i = 0; i < entriesUsed; i++) {
                writeFixedPointToBuffer(buffer, keys.get(i+1)); // Write Key[i]
                if ( i + 1 < entriesUsed) {
                    writeFixedPointToBuffer(buffer, childPointers.get(i)); // Write Child[i]
                }
            }
        }
    }

    public void parseBTreeAndLocalHeap(HdfLocalHeapContents localHeapContents) {
        if (nodeType != 0 || nodeLevel != 0) {
            throw new UnsupportedOperationException("Only nodeType=0 and nodeLevel=0 are supported.");
        }

        // Validate that the number of keys and children match the B-tree structure
        if (keys.size() != childPointers.size() + 1) {
            throw new IllegalStateException("Invalid B-tree structure: keys and children count mismatch.");
        }

        HdfString objectName = null;
        HdfFixedPoint childAddress = null;
        // Parse each key and corresponding child
        for (int i = 0; i < keys.size(); i++) {
            HdfFixedPoint keyOffset = keys.get(i);
            objectName = localHeapContents.parseStringAtOffset(keyOffset);

            if (i < childPointers.size()) {
                childAddress = childPointers.get(i);
            }
        }
        this.groupNodes = Collections.singletonList(new BtreeV1GroupNode(objectName, childAddress));
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
                ", groupNodes=" + (groupNodes==null?"NULL":groupNodes) +
                '}';
    }
}
