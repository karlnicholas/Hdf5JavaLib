package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfFileAllocation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;


@Getter
@Slf4j
public class HdfBTreeV1 {
    private final String signature;
    private final int nodeType;
    private final int nodeLevel;
    private int entriesUsed;
    private final HdfFixedPoint leftSiblingAddress;
    private final HdfFixedPoint rightSiblingAddress;
    private final HdfFixedPoint keyZero; // Kept as requested
    private final List<HdfBTreeEntry> entries; // Entries now contain SNODs or child B-trees

    // Keeping original constructors - though only the first one makes sense with recursive loading
    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfFixedPoint keyZero,
            List<HdfBTreeEntry> entries // This list is now populated recursively
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

    // This constructor might be less useful now, as entries are built recursively
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
        this.entries = new ArrayList<>(); // Will be empty initially
    }


    // --- Entry point for recursive reading ---
    public static HdfBTreeV1 readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize) throws IOException {
        long initialAddress = fileChannel.position();
        // Use a helper to handle recursion and prevent cycles
        return readFromFileChannelRecursive(fileChannel, initialAddress, offsetSize, lengthSize, new HashMap<>());
    }

    // --- Recursive Helper Method --- MODIFIED ---
    private static HdfBTreeV1 readFromFileChannelRecursive(FileChannel fileChannel,
                                                           long nodeAddress,
                                                           short offsetSize,
                                                           short lengthSize,
                                                           Map<Long, HdfBTreeV1> visitedNodes // Cycle detection/cache
    ) throws IOException { // Throws standard exceptions

        // --- Cycle Detection ---
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        // --- Position and Read Header ---
        // Let IO operations throw IOException directly if they fail.
        fileChannel.position(nodeAddress);
        long startPos = nodeAddress; // For error messages

        int headerSize = 8 + offsetSize + offsetSize;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int headerBytesRead = fileChannel.read(headerBuffer);
        if (headerBytesRead != headerSize) {
            throw new IOException("Could not read complete BTree header (" + headerSize + " bytes) at position " + startPos + ". Read only " + headerBytesRead + " bytes.");
        }
        headerBuffer.flip();

        byte[] signatureBytes = new byte[4];
        headerBuffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"TREE".equals(signature)) {
            // Original: Threw IllegalArgumentException. Keeping similar concept. IOException is also suitable.
            // FIX: Throw exception for invalid format. Using IOException for consistency with other read errors.
            throw new IOException("Invalid B-tree node signature: '" + signature + "' at position " + startPos);
        }

        int nodeType = Byte.toUnsignedInt(headerBuffer.get());
        int nodeLevel = Byte.toUnsignedInt(headerBuffer.get());
        int entriesUsed = Short.toUnsignedInt(headerBuffer.getShort());

        BitSet emptyBitset = new BitSet(); // Placeholder from original
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));

        // --- Read Key/Pointer Block ---
        int entriesDataSize = lengthSize + (entriesUsed * (offsetSize + lengthSize));
        // Add minimal sanity checks based on potential issues
        if (entriesUsed < 0 || entriesDataSize < lengthSize) {
            throw new IOException("Invalid BTree node parameters at position " + startPos + ": entriesUsed=" + entriesUsed);
        }
        long currentPosBeforeEntries = fileChannel.position();
        long fileSize = fileChannel.size();
        if (currentPosBeforeEntries + entriesDataSize > fileSize) {
            throw new IOException("Calculated BTree entriesDataSize (" + entriesDataSize + ") exceeds available file size at position " + currentPosBeforeEntries + " (Node: " + startPos + ")");
        }

        ByteBuffer entriesBuffer = ByteBuffer.allocate(entriesDataSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        if (entriesDataSize > 0) {
            int entryBytesRead = fileChannel.read(entriesBuffer);
            if (entryBytesRead != entriesDataSize) {
                // FIX: Throw clear IOException for incomplete read.
                throw new IOException("Could not read complete BTree entries data block (" + entriesDataSize + " bytes) at position " + currentPosBeforeEntries + ". Read only " + entryBytesRead + " bytes.");
            }
            entriesBuffer.flip();
        } else if (entriesUsed > 0) {
            // Should be caught by entriesUsed < 0 check, but defensive.
            throw new IOException("BTree node at " + startPos + " has entriesUsed=" + entriesUsed + " but entriesDataSize is 0.");
        }


        HdfFixedPoint keyZero = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);
        long filePosAfterEntriesBlock = fileChannel.position(); // Position after this node's entry block

        // --- Prepare Node and Mark Visited ---
        HdfBTreeV1 currentNode = new HdfBTreeV1(signature, nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries);
        visitedNodes.put(nodeAddress, currentNode); // Mark before recursion

        // --- Process Entries ---
        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfFixedPoint.readFromByteBuffer(entriesBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
            HdfFixedPoint key = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));
            long childAddress = childPointer.getInstance(Long.class);
            HdfBTreeEntry entry = null;

            // Check address validity
            if (childAddress < -1L || (childAddress != -1L && childAddress >= fileSize)) {
                throw new IOException("Invalid child address " + childAddress + " in BTree entry " + i
                        + " at node " + startPos + " (Level " + nodeLevel + "). File size is " + fileSize);
            }

            if (nodeLevel == 0) { // --- LEAF level node ---
                if (childAddress != -1L) {
                    fileChannel.position(childAddress);
                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
                    int snodEntriesToRead = snod.getNumberOfSymbols();
                    for (int e = 0; e < snodEntriesToRead; ++e) {
                        HdfSymbolTableEntry snodEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
                        snod.getSymbolTableEntries().add(snodEntry);
                    }
                    entry = new HdfBTreeEntry(key, childPointer, snod);
                    // Restore position for next BTree entry read *only if successful*
                    fileChannel.position(filePosAfterEntriesBlock);
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfGroupSymbolTableNode) null);
                }
            } else { // --- INTERNAL level node (nodeLevel > 0) ---
                if (childAddress != -1L) {
                    HdfBTreeV1 childNode = readFromFileChannelRecursive(fileChannel, childAddress, offsetSize, lengthSize, visitedNodes);
                    entry = new HdfBTreeEntry(key, childPointer, childNode);
                    // Restore position for next BTree entry read *only if successful*
                    fileChannel.position(filePosAfterEntriesBlock);
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfBTreeV1) null);
                }
            }
            // Add entry to list - only happens if no exception was thrown above
            entries.add(entry);
        }
        return currentNode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfBTreeV1{");
        sb.append("signature='").append(signature).append('\'');
        sb.append(", nodeType=").append(nodeType);
        sb.append(", nodeLevel=").append(nodeLevel);
        sb.append(", entriesUsed=").append(entriesUsed);
        sb.append(", leftSiblingAddress=").append(leftSiblingAddress);
        sb.append(", rightSiblingAddress=").append(rightSiblingAddress);
        sb.append(", keyZero=").append(keyZero);
        sb.append(", entries=["); // Start listing entries

        if (entries != null && !entries.isEmpty()) {
            boolean first = true;
            for (HdfBTreeEntry entry : entries) {
                if (!first) {
                    sb.append(", "); // Separator between entries
                }
                // Here, we call the toString() of HdfBTreeEntry
                // If HdfBTreeEntry doesn't override toString, this calls Object.toString()
                sb.append(entry); // Implicitly calls entry.toString()
                first = false;
            }
        } else if (entries != null) {
            // List is initialized but empty
            sb.append("<empty>");
        } else {
            // List is null
            sb.append("<null>");
        }

        sb.append("]"); // End listing entries
        sb.append('}');
        return sb.toString();
    }

    // Helper methods (Optional but Recommended)
    public boolean isLeafLevelNode() { return this.nodeLevel == 0; }
    public boolean isInternalLevelNode() { return this.nodeLevel > 0; }

    public int addGroup(HdfString objectName, HdfFixedPoint objectAddress, HdfLocalHeap localHeap, HdfLocalHeapContents localHeapContents, HdfGroupSymbolTableNode symbolTableNode) {
        if (entriesUsed >= 4) {
            throw new IllegalStateException("Cannot add more than 4 groups to this B-tree node.");
        }

        int linkNameOffset = localHeap.addToHeap(objectName, localHeapContents);
        HdfBTreeEntry newEntry = new HdfBTreeEntry(HdfFixedPoint.of(linkNameOffset), objectAddress, symbolTableNode);
        entries.add(newEntry);
        entriesUsed++;
        return linkNameOffset;
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
        buffer.put(signature.getBytes());
        buffer.put((byte) nodeType);
        buffer.put((byte) nodeLevel);
        buffer.putShort((short) entriesUsed);
        writeFixedPointToBuffer(buffer, leftSiblingAddress);
        writeFixedPointToBuffer(buffer, rightSiblingAddress);
        writeFixedPointToBuffer(buffer, keyZero);

        for (HdfBTreeEntry entry : entries) {
            writeFixedPointToBuffer(buffer, entry.getChildPointer());
            if (entry.getKey() != null) {
                writeFixedPointToBuffer(buffer, entry.getKey());
            }
            if (entry.getSymbolTableNode() != null) {
                buffer.position(entry.getChildPointer().getInstance(Long.class).intValue());
                entry.getSymbolTableNode().writeToBuffer(buffer);
            }
            // childBTree writing would need recursive logic if implemented
        }
    }
}

//package org.hdf5javalib.file.infrastructure;
//
//import lombok.Getter;
//import org.hdf5javalib.dataclass.HdfFixedPoint;
//import org.hdf5javalib.dataclass.HdfString;
//import org.hdf5javalib.file.HdfFileAllocation;
//
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.ArrayList;
//import java.util.BitSet;
//import java.util.List;
//
//import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;
//
//@Getter
//public class HdfBTreeV1 {
//    private final String signature;
//    private final int nodeType;
//    private final int nodeLevel;
//    private int entriesUsed;
//    private final HdfFixedPoint leftSiblingAddress;
//    private final HdfFixedPoint rightSiblingAddress;
//    private final HdfFixedPoint keyZero; // Key 0 (predefined)
//    private final List<HdfBTreeEntry> entries;
//
//    public HdfBTreeV1(
//            String signature,
//            int nodeType,
//            int nodeLevel,
//            int entriesUsed,
//            HdfFixedPoint leftSiblingAddress,
//            HdfFixedPoint rightSiblingAddress,
//            HdfFixedPoint keyZero,
//            List<HdfBTreeEntry> entries
//    ) {
//        this.signature = signature;
//        this.nodeType = nodeType;
//        this.nodeLevel = nodeLevel;
//        this.entriesUsed = entriesUsed;
//        this.leftSiblingAddress = leftSiblingAddress;
//        this.rightSiblingAddress = rightSiblingAddress;
//        this.keyZero = keyZero;
//        this.entries = entries;
//    }
//
//    public HdfBTreeV1(
//            String signature,
//            int nodeType,
//            int nodeLevel,
//            int entriesUsed,
//            HdfFixedPoint leftSiblingAddress,
//            HdfFixedPoint rightSiblingAddress
//    ) {
//        this.signature = signature;
//        this.nodeType = nodeType;
//        this.nodeLevel = nodeLevel;
//        this.entriesUsed = entriesUsed;
//        this.leftSiblingAddress = leftSiblingAddress;
//        this.rightSiblingAddress = rightSiblingAddress;
//        this.keyZero = HdfFixedPoint.of(0);
//        this.entries = new ArrayList<>();
//    }
//
//    public int addGroup(HdfFileAllocation hdfFileAllocation, HdfString objectName, HdfFixedPoint objectAddress, HdfLocalHeap localHeap, HdfLocalHeapContents localHeapContents, HdfGroupSymbolTableNode symbolTableNode) {
//        if (entriesUsed >= 4) {
//            throw new IllegalStateException("Cannot add more than 4 groups to this B-tree node.");
//        }
//
//        int linkNameOffset = localHeap.addToHeap(objectName, localHeapContents, hdfFileAllocation);
//        HdfBTreeEntry newEntry = new HdfBTreeEntry(HdfFixedPoint.of(linkNameOffset), objectAddress, symbolTableNode);
//        entries.add(newEntry);
//        entriesUsed++;
//        return linkNameOffset;
//    }
//
//    public static HdfBTreeV1 readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize) throws IOException {
//        ByteBuffer buffer = ByteBuffer.allocate(24).order(java.nio.ByteOrder.LITTLE_ENDIAN);
//        fileChannel.read(buffer);
//        buffer.flip();
//
//        byte[] signatureBytes = new byte[4];
//        buffer.get(signatureBytes);
//        String signature = new String(signatureBytes);
//        if (!"TREE".equals(signature)) {
//            throw new IllegalArgumentException("Invalid B-tree node signature: " + signature);
//        }
//
//        int nodeType = Byte.toUnsignedInt(buffer.get());
//        int nodeLevel = Byte.toUnsignedInt(buffer.get());
//        int entriesUsed = Short.toUnsignedInt(buffer.getShort());
//
//        BitSet emptyBitset = new BitSet();
//        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize)
//                ? HdfFixedPoint.undefined(buffer, offsetSize)
//                : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
//
//        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize)
//                ? HdfFixedPoint.undefined(buffer, offsetSize)
//                : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
//
//        // Adjust buffer size for internal nodes (extra child pointer)
//        int keyPointerBufferSize = lengthSize + (entriesUsed * (offsetSize + lengthSize)) + (nodeLevel > 0 ? offsetSize : 0);
//        buffer = ByteBuffer.allocate(keyPointerBufferSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);
//        fileChannel.read(buffer);
//        buffer.flip();
//
//        HdfFixedPoint keyZero = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));
//        List<HdfBTreeEntry> entries = new ArrayList<>();
//
//        if (nodeLevel == 0) { // Leaf node
//            for (int i = 0; i < entriesUsed; i++) {
//                HdfFixedPoint childPointer = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
//                HdfFixedPoint key = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));
//                long snodAddress = childPointer.getInstance(Long.class);
//                long currentPos = fileChannel.position();
//                fileChannel.position(snodAddress);
//                HdfGroupSymbolTableNode symbolTableNode = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
//                int entriesToRead = symbolTableNode.getNumberOfSymbols();
//                for (int e = 0; e < entriesToRead; ++e) {
//                    HdfSymbolTableEntry symbolTableEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
//                    symbolTableNode.getSymbolTableEntries().add(symbolTableEntry);
//                }
//                entries.add(new HdfBTreeEntry(key, childPointer, symbolTableNode));
//                fileChannel.position(currentPos);
//            }
//        } else if (nodeLevel > 0) { // Internal node
//            for (int i = 0; i < entriesUsed; i++) {
//                HdfFixedPoint childPointer = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
//                HdfFixedPoint key = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));
//                long childAddress = childPointer.getInstance(Long.class);
//                long currentPos = fileChannel.position();
//                fileChannel.position(childAddress);
//                HdfBTreeV1 childBTree = readFromFileChannel(fileChannel, offsetSize, lengthSize);
//                entries.add(new HdfBTreeEntry(key, childPointer, childBTree));
//                fileChannel.position(currentPos);
//            }
////            // Read the extra child pointer for internal nodes
////            if (entriesUsed > 0) {
////                HdfFixedPoint extraChildPointer = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
////                long childAddress = extraChildPointer.getInstance(Long.class);
////                long currentPos = fileChannel.position();
////                fileChannel.position(childAddress);
////                HdfBTreeV1 childBTree = readFromFileChannel(fileChannel, offsetSize, lengthSize);
////                entries.add(new HdfBTreeEntry(null, extraChildPointer, childBTree));
////                fileChannel.position(currentPos);
////            }
//        } else {
//            throw new IllegalStateException("Invalid node level: " + nodeLevel);
//        }
//
//        return new HdfBTreeV1(
//                signature,
//                nodeType,
//                nodeLevel,
//                entriesUsed,
//                leftSiblingAddress,
//                rightSiblingAddress,
//                keyZero,
//                entries
//        );
//    }
//
//    public void writeToByteBuffer(ByteBuffer buffer) {
//        buffer.put(signature.getBytes());
//        buffer.put((byte) nodeType);
//        buffer.put((byte) nodeLevel);
//        buffer.putShort((short) entriesUsed);
//        writeFixedPointToBuffer(buffer, leftSiblingAddress);
//        writeFixedPointToBuffer(buffer, rightSiblingAddress);
//        writeFixedPointToBuffer(buffer, keyZero);
//
//        for (HdfBTreeEntry entry : entries) {
//            writeFixedPointToBuffer(buffer, entry.getChildPointer());
//            if (entry.getKey() != null) {
//                writeFixedPointToBuffer(buffer, entry.getKey());
//            }
//            if (entry.getSymbolTableNode() != null) {
//                buffer.position(entry.getChildPointer().getInstance(Integer.class));
//                entry.getSymbolTableNode().writeToBuffer(buffer);
//            }
//            // childBTree writing would need recursive logic if implemented
//        }
//    }
//
//    public List<HdfBTreeEntry> getLeafEntries() {
//        List<HdfBTreeEntry> leafEntries = new ArrayList<>();
//        if (nodeLevel == 0) {
//            leafEntries.addAll(entries);
//        } else if (nodeLevel > 0) {
//            for (HdfBTreeEntry entry : entries) {
//                leafEntries.addAll(entry.getChildBTree().getLeafEntries());
//            }
//        }
//        return leafEntries;
//    }
//
//    @Override
//    public String toString() {
//        return "HdfBTreeV1{" +
//                "signature='" + signature + '\'' +
//                ", nodeType=" + nodeType +
//                ", nodeLevel=" + nodeLevel +
//                ", entriesUsed=" + entriesUsed +
//                ", leftSiblingAddress=" + leftSiblingAddress +
//                ", rightSiblingAddress=" + rightSiblingAddress +
//                ", keyZero=" + keyZero +
//                ", entries=" + entries +
//                '}';
//    }
//}