package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
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

    public static HdfBTreeV1 readFromFileChannel(SeekableByteChannel fileChannel, short offsetSize, short lengthSize, HdfDataFile hdfDataFile) throws IOException {
        long initialAddress = fileChannel.position();
        return readFromFileChannelRecursive(fileChannel, initialAddress, offsetSize, lengthSize, new HashMap<>(), hdfDataFile);
    }

    private static HdfBTreeV1 readFromFileChannelRecursive(SeekableByteChannel fileChannel,
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
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
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

        ByteBuffer entriesBuffer = ByteBuffer.allocate(entriesDataSize).order(ByteOrder.LITTLE_ENDIAN);
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
            HdfBTreeEntry entry;

            if (childAddress < -1L || (childAddress != -1L && childAddress >= fileSize)) {
                throw new IOException("Invalid child address " + childAddress + " in BTree entry " + i
                        + " at node " + startPos + " (Level " + nodeLevel + "). File size is " + fileSize);
            }

            if (nodeLevel == 0) {
                if (childAddress != -1L) {
                    fileChannel.position(childAddress);
                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
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

    public void addDataset(
            long linkNameOffset,
            long datasetObjectHeaderAddress,
            String datasetName,
            HdfGroup group
    ) {
        if (!isLeafLevelNode()) {
            throw new IllegalStateException("addDataset can only be called on leaf B-tree nodes (nodeLevel 0).");
        }
        if (datasetName == null || datasetName.isEmpty()) {
            throw new IllegalArgumentException("Dataset name cannot be null or empty");
        }

        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        final int MAX_SNOD_ENTRIES = 8;
        HdfGroupSymbolTableNode targetSnod = null;
        HdfBTreeEntry targetEntry = null;
        int targetEntryIndex = -1;
        int insertIndex = 0;

        // Step 1: Find target SNOD using binary search
        if (entries.isEmpty()) {
            // Create new SNOD (first dataset)
            long snodOffset = fileAllocation.allocateNextSnodStorage();
            targetSnod = new HdfGroupSymbolTableNode("SNOD", 1, new ArrayList<>(MAX_SNOD_ENTRIES));
            HdfFixedPoint key = HdfFixedPoint.of(linkNameOffset);
            HdfFixedPoint childPointer = HdfFixedPoint.of(snodOffset);
            targetEntry = new HdfBTreeEntry(key, childPointer, targetSnod);
            entries.add(targetEntry);
            entriesUsed++;
            targetEntryIndex = 0;
            insertIndex = 0;
        } else {
            int low = 0;
            int high = entries.size() - 1;
            int insertionPoint = entries.size();

            while (low <= high) {
                int mid = low + ((high - low) >>> 1);  // Avoid overflow
                HdfBTreeEntry currentEntry = entries.get(mid);

                // Get key and resolve to the max dataset name in this SNOD
                long maxOffset = currentEntry.getKey().getInstance(Long.class); // adapt if it's just getKey()
                String maxName = group.getDatasetNameByLinkNameOffset(maxOffset);

                int cmp = datasetName.compareTo(maxName);
                if (cmp <= 0) {
                    insertionPoint = mid;
                    high = mid - 1;
                } else {
                    low = mid + 1;
                }
            }

            int targetIndex = (insertionPoint == entries.size()) ? entries.size() - 1 : insertionPoint;

            // 'targetSnodIndex' now holds the index of the HdfBTreeEntry in the non-empty 'entries' list
            // that should be considered first for inserting the new 'datasetName'.
            // This index is determined assuming 'entries' is sorted by 'maxName'.
            // Further logic outside this search block will handle capacity checks and splits.

            // Select SNOD based on targetSnodIndex
            targetEntry = entries.get(targetIndex);
            targetSnod = targetEntry.getSymbolTableNode();
            targetEntryIndex = targetIndex;

            // Find insertion index within SNOD
            List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();
            for (int j = 0; j < symbolTableEntries.size(); j++) {
                String existingName = group.getDatasetNameByLinkNameOffset(symbolTableEntries.get(j).getLinkNameOffset().getInstance(Long.class));
                if (datasetName.compareTo(existingName) < 0) {
                    insertIndex = j;
                    break;
                }
                insertIndex = j + 1;
            }
        }

        // Step 2: Insert HdfSymbolTableEntry
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
                HdfFixedPoint.of(linkNameOffset),
                HdfFixedPoint.of(datasetObjectHeaderAddress)
        );
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();
        symbolTableEntries.add(insertIndex, ste);

        // Step 3: Update B-tree key if necessary
        String newMaxName = symbolTableEntries.stream()
                .map(e -> group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class)))
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .orElseThrow(() -> new IllegalStateException("No valid dataset names in SNOD"));
        long maxLinkNameOffset = symbolTableEntries.stream()
                .filter(e -> newMaxName.equals(group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class))))
                .findFirst()
                .map(e -> e.getLinkNameOffset().getInstance(Long.class))
                .orElseThrow(() -> new IllegalStateException("Could not find linkNameOffset for max dataset name: " + newMaxName));
        targetEntry.setKey(HdfFixedPoint.of(maxLinkNameOffset));

        // Step 4: Handle SNOD split if full
        if (symbolTableEntries.size() > MAX_SNOD_ENTRIES) {
            splitSnod(targetEntryIndex, fileAllocation, group);
        }
    }

    private void splitSnod(int targetEntryIndex, HdfFileAllocation fileAllocation, HdfGroup group) {
        final int MAX_SNOD_ENTRIES = 8;
        HdfBTreeEntry targetEntry = entries.get(targetEntryIndex);
        HdfGroupSymbolTableNode targetSnod = targetEntry.getSymbolTableNode();
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();

        // Create new SNOD
        long newSnodOffset = fileAllocation.allocateNextSnodStorage();
        HdfGroupSymbolTableNode newSnod = new HdfGroupSymbolTableNode("SNOD", 1, new ArrayList<>(MAX_SNOD_ENTRIES));

        // Redistribute entries: first 4 to target SNOD, last 5 to new SNOD
        List<HdfSymbolTableEntry> retainedEntries = new ArrayList<>(symbolTableEntries.subList(0, 4));
        List<HdfSymbolTableEntry> movedEntries = new ArrayList<>(symbolTableEntries.subList(4, symbolTableEntries.size()));

        // Update target SNOD
        symbolTableEntries.clear();
        symbolTableEntries.addAll(retainedEntries);

        // Update new SNOD
        List<HdfSymbolTableEntry> newSnodEntries = newSnod.getSymbolTableEntries();
        newSnodEntries.addAll(movedEntries);

        // Update key for target SNOD
        String targetMaxName = symbolTableEntries.stream()
                .map(e -> group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class)))
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .orElseThrow(() -> new IllegalStateException("No valid dataset names in target SNOD"));
        long targetMaxLinkNameOffset = symbolTableEntries.stream()
                .filter(e -> targetMaxName.equals(group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class))))
                .findFirst()
                .map(e -> e.getLinkNameOffset().getInstance(Long.class))
                .orElseThrow(() -> new IllegalStateException("Could not find linkNameOffset for max dataset name: " + targetMaxName));
        targetEntry.setKey(HdfFixedPoint.of(targetMaxLinkNameOffset));

        // Create new BTreeEntry
        String newMaxName = newSnodEntries.stream()
                .map(e -> group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class)))
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .orElseThrow(() -> new IllegalStateException("No valid dataset names in new SNOD"));
        long newMaxLinkNameOffset = newSnodEntries.stream()
                .filter(e -> newMaxName.equals(group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class))))
                .findFirst()
                .map(e -> e.getLinkNameOffset().getInstance(Long.class))
                .orElseThrow(() -> new IllegalStateException("Could not find linkNameOffset for max dataset name: " + newMaxName));
        HdfFixedPoint newKey = HdfFixedPoint.of(newMaxLinkNameOffset);
        HdfFixedPoint newChildPointer = HdfFixedPoint.of(newSnodOffset);
        HdfBTreeEntry newEntry = new HdfBTreeEntry(newKey, newChildPointer, newSnod);

        // Insert new BTreeEntry in sorted order
        int insertPos = targetEntryIndex + 1;
        for (int i = targetEntryIndex + 1; i < entries.size(); i++) {
            String maxName = group.getDatasetNameByLinkNameOffset(entries.get(i).getKey().getInstance(Long.class));
            if (maxName == null) {
                throw new IllegalStateException("No dataset name found for key linkNameOffset: " + entries.get(i).getKey().getInstance(Long.class));
            }
            if (newMaxName.compareTo(maxName) < 0) {
                break;
            }
            insertPos++;
        }
        entries.add(insertPos, newEntry);
        entriesUsed++;
    }

    public boolean isLeafLevelNode() { return this.nodeLevel == 0; }
    public boolean isInternalLevelNode() { return this.nodeLevel > 0; }

    public void writeToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate((int) fileAllocation.getBtreeTotalSize()).order(ByteOrder.LITTLE_ENDIAN);
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
        buffer.rewind();
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }
    }

    public Map<Long, HdfGroupSymbolTableNode> mapOffsetToSnod() {
        Map<Long, HdfGroupSymbolTableNode> offsetToSnodMap = new HashMap<>();
        collectSnodsRecursively(this, offsetToSnodMap);
        return offsetToSnodMap;
    }

    private void collectSnodsRecursively(HdfBTreeV1 node, Map<Long, HdfGroupSymbolTableNode> map) {
        if (node == null || node.getEntries() == null) {
            return;
        }

        if (node.isLeafLevelNode()) {
            for (HdfBTreeEntry entry : entries) {
                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
                if (snod != null) {
                    long offset = entry.getChildPointer().getInstance(Long.class);
                    if (offset != -1L) {
                        map.put(offset, snod);
                    }
                }
            }
        } else {
            for (HdfBTreeEntry entry : entries) {
                HdfBTreeV1 childBTree = entry.getChildBTree();
                if (childBTree != null) {
                    collectSnodsRecursively(childBTree, map);
                }
            }
        }
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
}