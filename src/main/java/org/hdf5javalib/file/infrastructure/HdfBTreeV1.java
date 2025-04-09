package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;

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
    private final HdfFixedPoint keyZero;
    private final List<HdfBTreeEntry> entries;
    private final HdfDataFile hdfDataFile;

    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfFixedPoint keyZero,
            List<HdfBTreeEntry> entries,
            HdfDataFile hdfDataFile
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = keyZero;
        this.entries = entries;
        this.hdfDataFile = hdfDataFile;
    }

    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfDataFile hdfDataFile
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.hdfDataFile = hdfDataFile;
        this.entriesUsed = 0;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = HdfFixedPoint.of(0);
        this.entries = new ArrayList<>();
    }

    public static HdfBTreeV1 readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize, HdfDataFile hdfDataFile) throws IOException {
        long initialAddress = fileChannel.position();
        return readFromFileChannelRecursive(fileChannel, initialAddress, offsetSize, lengthSize, new HashMap<>(), hdfDataFile);
    }

    private static HdfBTreeV1 readFromFileChannelRecursive(FileChannel fileChannel,
                                                           long nodeAddress,
                                                           short offsetSize,
                                                           short lengthSize,
                                                           Map<Long, HdfBTreeV1> visitedNodes,
                                                           HdfDataFile hdfDataFile
    ) throws IOException {
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        fileChannel.position(nodeAddress);
        long startPos = nodeAddress;

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
            throw new IOException("Invalid B-tree node signature: '" + signature + "' at position " + startPos);
        }

        int nodeType = Byte.toUnsignedInt(headerBuffer.get());
        int nodeLevel = Byte.toUnsignedInt(headerBuffer.get());
        int entriesUsed = Short.toUnsignedInt(headerBuffer.getShort());

        BitSet emptyBitset = new BitSet();
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));

        int entriesDataSize = lengthSize + (entriesUsed * (offsetSize + lengthSize));
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
                throw new IOException("Could not read complete BTree entries data block (" + entriesDataSize + " bytes) at position " + currentPosBeforeEntries + ". Read only " + entryBytesRead + " bytes.");
            }
            entriesBuffer.flip();
        } else if (entriesUsed > 0) {
            throw new IOException("BTree node at " + startPos + " has entriesUsed=" + entriesUsed + " but entriesDataSize is 0.");
        }

        HdfFixedPoint keyZero = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);
        long filePosAfterEntriesBlock = fileChannel.position();

        HdfBTreeV1 currentNode = new HdfBTreeV1(signature, nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries, hdfDataFile);
        visitedNodes.put(nodeAddress, currentNode);

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfFixedPoint.readFromByteBuffer(entriesBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
            HdfFixedPoint key = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));
            long childAddress = childPointer.getInstance(Long.class);
            HdfBTreeEntry entry = null;

            if (childAddress < -1L || (childAddress != -1L && childAddress >= fileSize)) {
                throw new IOException("Invalid child address " + childAddress + " in BTree entry " + i
                        + " at node " + startPos + " (Level " + nodeLevel + "). File size is " + fileSize);
            }

            if (nodeLevel == 0) {
                if (childAddress != -1L) {
                    fileChannel.position(childAddress);
                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
                    int snodEntriesToRead = snod.getNumberOfSymbols();
                    for (int e = 0; e < snodEntriesToRead; ++e) {
                        HdfSymbolTableEntry snodEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
                        snod.getSymbolTableEntries().add(snodEntry);
                    }
                    entry = new HdfBTreeEntry(key, childPointer, snod);
                    fileChannel.position(filePosAfterEntriesBlock);
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfGroupSymbolTableNode) null);
                }
            } else {
                if (childAddress != -1L) {
                    HdfBTreeV1 childNode = readFromFileChannelRecursive(fileChannel, childAddress, offsetSize, lengthSize, visitedNodes, hdfDataFile);
                    entry = new HdfBTreeEntry(key, childPointer, childNode);
                    fileChannel.position(filePosAfterEntriesBlock);
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfBTreeV1) null);
                }
            }
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
        sb.append(", entries=[");
        if (entries != null && !entries.isEmpty()) {
            boolean first = true;
            for (HdfBTreeEntry entry : entries) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(entry);
                first = false;
            }
        } else if (entries != null) {
            sb.append("<empty>");
        } else {
            sb.append("<null>");
        }
        sb.append("]");
        sb.append('}');
        return sb.toString();
    }

    public boolean isLeafLevelNode() { return this.nodeLevel == 0; }
    public boolean isInternalLevelNode() { return this.nodeLevel > 0; }

    public void addDataset(long linkNameOffset, long datasetObjectHeaderAddress) {
        if (!isLeafLevelNode()) {
            throw new IllegalStateException("addDataset can only be called on leaf B-tree nodes (nodeLevel 0).");
        }

        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        HdfGroupSymbolTableNode targetSnod;

        if (entries.isEmpty()) {
            if (entriesUsed != 0) {
                throw new IllegalStateException("B-tree entries list is empty but entriesUsed is " + entriesUsed);
            }

            final int MAX_SNOD_ENTRIES = 8;
            long snodOffset = fileAllocation.allocateNextSnodStorage();
            targetSnod = new HdfGroupSymbolTableNode("SNOD", 1, 0, new ArrayList<>(MAX_SNOD_ENTRIES));
            HdfFixedPoint key = HdfFixedPoint.of(linkNameOffset);
            HdfFixedPoint childPointer = HdfFixedPoint.of(snodOffset);
            HdfBTreeEntry newEntry = new HdfBTreeEntry(key, childPointer, targetSnod);
            entries.add(newEntry);
            entriesUsed++;
        } else {
            if (entriesUsed != 1) {
                throw new IllegalStateException("addDataset currently only supports adding to the SNOD within the first B-tree entry. entriesUsed=" + entriesUsed);
            }
            HdfBTreeEntry firstEntry = entries.get(0);
            targetSnod = firstEntry.getSymbolTableNode();
            if (targetSnod == null) {
                throw new IllegalStateException("The first B-tree entry does not contain a Symbol Table Node.");
            }
        }

        if (targetSnod.getNumberOfSymbols() >= 8) {
            throw new IllegalStateException("Cannot add more than 8 datasets to this Symbol Table Node.");
        }

        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
                HdfFixedPoint.of(linkNameOffset),
                HdfFixedPoint.of(datasetObjectHeaderAddress)
        );
        targetSnod.addEntry(ste);
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        buffer.position((int)(fileAllocation.getBtreeOffset() - fileAllocation.getRootGroupOffset()));
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
            } else {
                throw new NullPointerException("BTree Entry key cannot be null during write");
            }
        }
    }

    /**
     * Returns a map of SNOD offsets to their corresponding HdfGroupSymbolTableNode instances.
     * Traverses the B-tree recursively to collect all SNODs from leaf nodes.
     *
     * @return A Map where keys are SNOD offsets (from childPointer) and values are HdfGroupSymbolTableNode instances.
     */
    public Map<Long, HdfGroupSymbolTableNode> mapOffsetToSnod() {
        Map<Long, HdfGroupSymbolTableNode> offsetToSnodMap = new HashMap<>();
        collectSnodsRecursively(this, offsetToSnodMap);
        return offsetToSnodMap;
    }

    /**
     * Helper method to recursively collect SNODs from the B-tree.
     *
     * @param node The current B-tree node to process.
     * @param map  The map to populate with SNOD offsets and nodes.
     */
    private void collectSnodsRecursively(HdfBTreeV1 node, Map<Long, HdfGroupSymbolTableNode> map) {
        if (node == null || node.getEntries() == null) {
            return;
        }

        if (node.isLeafLevelNode()) {
            // Leaf node: collect SNODs from entries
            for (HdfBTreeEntry entry : node.getEntries()) {
                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
                if (snod != null) {
                    long offset = entry.getChildPointer().getInstance(Long.class);
                    if (offset != -1L) { // Skip undefined pointers
                        map.put(offset, snod);
                    }
                }
            }
        } else {
            // Internal node: recurse into child B-trees
            for (HdfBTreeEntry entry : node.getEntries()) {
                HdfBTreeV1 childBTree = entry.getChildBTree();
                if (childBTree != null) {
                    collectSnodsRecursively(childBTree, map);
                }
            }
        }
    }
}