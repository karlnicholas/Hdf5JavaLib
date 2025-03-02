package org.hdf5javalib.file.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
public class HdfBTreeV1 {
    private final String signature;
    private final int nodeType;
    private final int nodeLevel;
    private int entriesUsed;
    private final HdfFixedPoint leftSiblingAddress;
    private final HdfFixedPoint rightSiblingAddress;
    private final HdfFixedPoint keyZero; // Key 0 (predefined)
    private final List<HdfBTreeEntry> entries;

    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfFixedPoint keyZero,
            List<HdfBTreeEntry> entries
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = keyZero;
        this.entries = entries;
    }

    public HdfBTreeV1(
        String signature,
        int nodeType,
        int nodeLevel,
        int entriesUsed,
        HdfFixedPoint leftSiblingAddress,
        HdfFixedPoint rightSiblingAddress
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = HdfFixedPoint.of(0);
        this.entries = new ArrayList<>();
    }

    public int addGroup(HdfString objectName, HdfFixedPoint objectAddress, HdfLocalHeap localHeap, HdfLocalHeapContents localHeapContents) {
        // Ensure we do not exceed the max number of entries (groupLeafNodeK = 4)
        if (entriesUsed >= 4) {
            throw new IllegalStateException("Cannot add more than 4 groups to this B-tree node.");
        }

        // Store objectName in the heap & get its offset
        int linkNameOffset = localHeap.addToHeap(objectName, localHeapContents);
//        HdfFixedPoint localHeapOffset = localHeap.getFreeListOffset();

        // Insert `HdfBTreeEntry` for the new group
        HdfBTreeEntry newEntry = new HdfBTreeEntry(HdfFixedPoint.of(linkNameOffset), objectAddress);
        entries.add(newEntry);

        // Increment entriesUsed (since we successfully added an entry)
        entriesUsed++;
        return linkNameOffset;
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

        BitSet emptyBitset = new BitSet();
        // Read sibling addresses
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize)
                ? HdfFixedPoint.undefined(buffer, offsetSize)
                : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));

        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize)
                ? HdfFixedPoint.undefined(buffer, offsetSize)
                : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));

        // Corrected Buffer Allocation (Always allocate for keyZero + keys/childPointers)
        int keyPointerBufferSize = lengthSize + (entriesUsed * (offsetSize + lengthSize));
        buffer = ByteBuffer.allocate(keyPointerBufferSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);

        // Read keys and child pointers
        fileChannel.read(buffer);
        buffer.flip();

        // Always Read keyZero (first key)
        HdfFixedPoint keyZero = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));

        // Read remaining entries (Child Pointer first, then Key)
        List<HdfBTreeEntry> entries = new ArrayList<>();

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8)); // Read childPointer first
            HdfFixedPoint key = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8)); // Read key after childPointer
            entries.add(new HdfBTreeEntry(key, childPointer));
        }

        return new HdfBTreeV1(
                signature,
                nodeType,
                nodeLevel,
                entriesUsed,
                leftSiblingAddress,
                rightSiblingAddress,
                keyZero,
                entries
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

        // Step 7: Write keyZero (Always needs to be written)
        writeFixedPointToBuffer(buffer, keyZero);

        // Step 8: Write remaining entries (Keys and Child Pointers)
        for (HdfBTreeEntry entry : entries) {
            writeFixedPointToBuffer(buffer, entry.getChildPointer());  // Write child pointer
            writeFixedPointToBuffer(buffer, entry.getKey());  // Write key
        }
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
                ", keyZero=" + keyZero +
                ", entries=" + entries +
                '}';
    }
}
