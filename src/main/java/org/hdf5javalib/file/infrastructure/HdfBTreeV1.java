package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfFileAllocation;
// Imports kept minimal

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map; // For recursion tracking
import java.util.HashMap; // For recursion tracking

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;


@Getter
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

    // --- Recursive Helper Method ---
    private static HdfBTreeV1 readFromFileChannelRecursive(FileChannel fileChannel,
                                                           long nodeAddress,
                                                           short offsetSize,
                                                           short lengthSize,
                                                           Map<Long, HdfBTreeV1> visitedNodes // Cycle detection/cache
    ) throws IOException {

        // --- Cycle Detection / Cache ---
        if (visitedNodes.containsKey(nodeAddress)) {
            System.err.println("Warning: Detected potential cycle or re-visit of BTree node at address " + nodeAddress + ". Returning cached node.");
            return visitedNodes.get(nodeAddress); // Return previously read node
        }

        long originalPos = fileChannel.position(); // Save original position before we seek
        fileChannel.position(nodeAddress); // Seek to the node we need to read
        long startPos = nodeAddress; // For logging/error messages

        // System.out.println("DEBUG: Recursively reading BTree Node at: " + startPos);

        // Read Header
        int headerSize = 8 + offsetSize + offsetSize;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int headerBytesRead = fileChannel.read(headerBuffer);
        if (headerBytesRead != headerSize) {
            fileChannel.position(originalPos); // Restore position on error
            throw new IOException("Could not read BTree header at position " + startPos);
        }
        headerBuffer.flip();

        byte[] signatureBytes = new byte[4];
        headerBuffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"TREE".equals(signature)) {
            fileChannel.position(originalPos); // Restore position on error
            throw new IllegalArgumentException("Invalid B-tree node signature: " + signature + " at position " + startPos);
        }

        int nodeType = Byte.toUnsignedInt(headerBuffer.get());
        int nodeLevel = Byte.toUnsignedInt(headerBuffer.get());
        int entriesUsed = Short.toUnsignedInt(headerBuffer.getShort());

        BitSet emptyBitset = new BitSet();
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));

        // Read Key/Pointer block
        int entriesDataSize = lengthSize + (entriesUsed * (offsetSize + lengthSize));
        ByteBuffer entriesBuffer = ByteBuffer.allocate(entriesDataSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int entryBytesRead = fileChannel.read(entriesBuffer);
        if (entryBytesRead != entriesDataSize) {
            fileChannel.position(originalPos); // Restore position on error
            throw new IOException("Could not read BTree entries data block (" + entriesDataSize + " bytes) at position " + fileChannel.position());
        }
        entriesBuffer.flip();

        HdfFixedPoint keyZero = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);
        long filePosAfterEntriesBlock = fileChannel.position(); // Position AFTER reading the entries buffer from the channel


        // --- Create a placeholder node and add to visited map BEFORE recursion ---
        // This helps break cycles if node refers back to itself or an ancestor
        // We'll populate the entries list below.
        HdfBTreeV1 currentNode = new HdfBTreeV1(signature, nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries);
        visitedNodes.put(nodeAddress, currentNode);


        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfFixedPoint.readFromByteBuffer(entriesBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
            HdfFixedPoint key = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));

            long childAddress = childPointer.getInstance(Long.class);
            HdfBTreeEntry entry = null;

            if (nodeLevel == 0) { // --- LEAF level node ---
                HdfGroupSymbolTableNode snod = null;
                if (childAddress != -1L) { // Assuming -1L is undefined
                    long currentFilePosBeforeSnod = fileChannel.position(); // Save before SNOD jump
                    try {
                        fileChannel.position(childAddress);
                        snod = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
                        int snodEntriesToRead = snod.getNumberOfSymbols();
                        for (int e = 0; e < snodEntriesToRead; ++e) {
                            HdfSymbolTableEntry snodEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
                            snod.getSymbolTableEntries().add(snodEntry);
                        }
                        entry = new HdfBTreeEntry(key, childPointer, snod); // Create entry with SNOD
                    } catch (IOException e) {
                        System.err.println("Error reading Symbol Table Node (or entries) at address " + childAddress + " for BTree leaf entry " + i + ": " + e.getMessage());
                        entry = new HdfBTreeEntry(key, childPointer, (HdfGroupSymbolTableNode) null); // Entry with null SNOD on error
                    } finally {
                        // Restore position to after the BTree entries block, regardless of SNOD read success/failure
                        fileChannel.position(filePosAfterEntriesBlock);
                    }
                } else {
                    System.err.println("Warning: BTree Leaf entry " + i + " (Level 0) has undefined child address. Creating entry with null SNOD.");
                    entry = new HdfBTreeEntry(key, childPointer, (HdfGroupSymbolTableNode) null); // Entry with null SNOD
                }

            } else { // --- INTERNAL level node (nodeLevel > 0) ---
                HdfBTreeV1 childNode = null;
                if (childAddress != -1L) {
                    try {
                        // --- Recursive Call ---
                        // Pass the child address and the visited map down
                        // Position will be handled inside the recursive call
                        childNode = readFromFileChannelRecursive(fileChannel, childAddress, offsetSize, lengthSize, visitedNodes);
                        entry = new HdfBTreeEntry(key, childPointer, childNode); // Create entry with child BTree
                    } catch (IOException e) {
                        System.err.println("Error recursively reading child BTree Node at address " + childAddress + " for BTree internal entry " + i + ": " + e.getMessage());
                        entry = new HdfBTreeEntry(key, childPointer, (HdfBTreeV1) null); // Entry with null child on error
                    }
                    // NOTE: Position is automatically restored after the recursive call finishes
                    // because the recursive call itself restores the position it started with.
                    // Ensure fileChannel position is correct for next iteration (should be after the main BTree entries block)
                    fileChannel.position(filePosAfterEntriesBlock);

                } else {
                    System.err.println("Warning: BTree Internal entry " + i + " (Level " + nodeLevel + ") has undefined child address. Creating entry with null child BTree.");
                    entry = new HdfBTreeEntry(key, childPointer, (HdfBTreeV1) null); // Entry with null child BTree
                }
            }
            // Add the created entry (which might have null payload if read failed)
            if (entry != null) {
                entries.add(entry);
            } else {
                // Should not happen if logic above is correct, but as a failsafe:
                System.err.println("INTERNAL ERROR: Failed to create HdfBTreeEntry for BTree node at "+startPos+", entry index "+i);
            }
        }

        // Restore the original file position that the channel had when this function was called
        fileChannel.position(originalPos);

        // Return the fully populated node (its `entries` list has been filled)
        return currentNode;
    }


    // writeToByteBuffer would need significant changes to handle writing recursively
    // public void writeToByteBuffer(ByteBuffer buffer) { ... }

    // toString can remain, but might become very large if printed
    @Override
    public String toString() {
        // Consider a less verbose toString for recursive structures
        return "HdfBTreeV1{" +
                "signature='" + signature + '\'' +
                ", nodeType=" + nodeType +
                ", nodeLevel=" + nodeLevel +
                ", entriesUsed=" + entriesUsed +
                ", leftSiblingAddress=" + leftSiblingAddress +
                ", rightSiblingAddress=" + rightSiblingAddress +
                ", keyZero=" + keyZero +
                ", entries.size=" + entries.size() + // Avoid printing full recursive structure
                '}';
    }

    // Helper methods (Optional but Recommended)
    public boolean isLeafLevelNode() { return this.nodeLevel == 0; }
    public boolean isInternalLevelNode() { return this.nodeLevel > 0; }

    public int addGroup(HdfFileAllocation hdfFileAllocation, HdfString objectName, HdfFixedPoint objectAddress, HdfLocalHeap localHeap, HdfLocalHeapContents localHeapContents, HdfGroupSymbolTableNode symbolTableNode) {
        if (entriesUsed >= 4) {
            throw new IllegalStateException("Cannot add more than 4 groups to this B-tree node.");
        }

        int linkNameOffset = localHeap.addToHeap(objectName, localHeapContents, hdfFileAllocation);
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
                buffer.position(entry.getChildPointer().getInstance(Integer.class));
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