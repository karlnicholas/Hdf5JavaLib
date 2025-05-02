package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.utils.HdfReadUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.function.Function;

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
        this.keyZero = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFixedPointDatatypeForLength());
        this.entries = new ArrayList<>();
    }

    public static HdfBTreeV1 readFromFileChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws IOException {
        long initialAddress = fileChannel.position();
        return readFromFileChannelRecursive(fileChannel, initialAddress, new HashMap<>(), hdfDataFile);
    }

    private static HdfBTreeV1 readFromFileChannelRecursive(SeekableByteChannel fileChannel,
                                                           long nodeAddress,
                                                           Map<Long, HdfBTreeV1> visitedNodes,
                                                           HdfDataFile hdfDataFile
    ) throws IOException {
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        fileChannel.position(nodeAddress);
        long startPos = nodeAddress;

//        int headerSize = 8 + offsetSize + offsetSize;
        int headerSize = 8 + hdfDataFile.getFixedPointDatatypeForOffset().getSize() + hdfDataFile.getFixedPointDatatypeForOffset().getSize();
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
        HdfFixedPoint leftSiblingAddress = HdfReadUtils.checkUndefined(headerBuffer, hdfDataFile.getFixedPointDatatypeForOffset().getSize()) ? hdfDataFile.getFixedPointDatatypeForOffset().undefined(headerBuffer) : HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), headerBuffer);
        HdfFixedPoint rightSiblingAddress = HdfReadUtils.checkUndefined(headerBuffer, hdfDataFile.getFixedPointDatatypeForOffset().getSize()) ? hdfDataFile.getFixedPointDatatypeForOffset().undefined(headerBuffer) : HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), headerBuffer);

        int entriesDataSize = hdfDataFile.getFixedPointDatatypeForLength().getSize()
                + (entriesUsed * (hdfDataFile.getFixedPointDatatypeForOffset().getSize() + hdfDataFile.getFixedPointDatatypeForLength().getSize()));
        if (entriesUsed < 0 || entriesDataSize < hdfDataFile.getFixedPointDatatypeForLength().getSize()) {
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

        HdfFixedPoint keyZero = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForLength(), entriesBuffer);

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);
        long filePosAfterEntriesBlock = fileChannel.position();

        HdfBTreeV1 currentNode = new HdfBTreeV1(signature, nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries, hdfDataFile);
        visitedNodes.put(nodeAddress, currentNode);

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), entriesBuffer);
            HdfFixedPoint key = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForLength(), entriesBuffer);
            long childAddress = childPointer.getInstance(Long.class);
            HdfBTreeEntry entry;

            if (childAddress < -1L || (childAddress != -1L && childAddress >= fileSize)) {
                throw new IOException("Invalid child address " + childAddress + " in BTree entry " + i
                        + " at node " + startPos + " (Level " + nodeLevel + "). File size is " + fileSize);
            }

            if (nodeLevel == 0) {
                if (childAddress != -1L) {
                    fileChannel.position(childAddress);
                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, hdfDataFile);
                    entry = new HdfBTreeEntry(key, childPointer, snod);
                    fileChannel.position(filePosAfterEntriesBlock);
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfGroupSymbolTableNode) null);
                }
            } else {
                if (childAddress != -1L) {
                    HdfBTreeV1 childNode = readFromFileChannelRecursive(fileChannel, childAddress, visitedNodes, hdfDataFile);
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
        HdfGroupSymbolTableNode targetSnod;
        HdfBTreeEntry targetEntry;
        int targetSnodIndex;
        int insertIndex;

        // --- Step 1: Find or create target SNOD ---
        if (entries.isEmpty()) {
            long snodOffset = fileAllocation.allocateNextSnodStorage();
            targetSnod = new HdfGroupSymbolTableNode("SNOD", 1, new ArrayList<>(MAX_SNOD_ENTRIES));
            targetEntry = new HdfBTreeEntry(
                    HdfWriteUtils.hdfFixedPointFromValue(linkNameOffset, hdfDataFile.getFixedPointDatatypeForOffset()),
                    HdfWriteUtils.hdfFixedPointFromValue(snodOffset, hdfDataFile.getFixedPointDatatypeForOffset()), targetSnod);
            entries.add(targetEntry);
            entriesUsed++;
            targetSnodIndex = 0;
            insertIndex = 0;
        } else {
            // instead of the full while loop over entries:
            targetSnodIndex = binarySearchDatasetName(entries.size(), (mid)->{
                HdfBTreeEntry entry = entries.get(mid);
                long maxOffset = entry.getKey().getInstance(Long.class);
                String maxName = group.getDatasetNameByLinkNameOffset(maxOffset);
                return datasetName.compareTo(maxName);
            });
            targetSnodIndex = targetSnodIndex == entries.size() ? entries.size() - 1 : targetSnodIndex;

            targetEntry = entries.get(targetSnodIndex);
            targetSnod = targetEntry.getSymbolTableNode();

            // --- Step 2: Binary search within SNOD's symbol table ---
            insertIndex = binarySearchDatasetName(targetSnod.getSymbolTableEntries().size(), (mid)->{
                HdfSymbolTableEntry ste = targetSnod.getSymbolTableEntries().get(mid);
                long offset = ste.getLinkNameOffset().getInstance(Long.class);
                String name = group.getDatasetNameByLinkNameOffset(offset);
                return datasetName.compareTo(name);
            });
        }

        // --- Step 3: Insert new dataset ---
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
            HdfWriteUtils.hdfFixedPointFromValue(linkNameOffset, hdfDataFile.getFixedPointDatatypeForOffset()),
            HdfWriteUtils.hdfFixedPointFromValue(datasetObjectHeaderAddress, hdfDataFile.getFixedPointDatatypeForOffset())
        );
        targetSnod.getSymbolTableEntries().add(insertIndex, ste);

        // --- Step 4: Update B-tree key with new max ---
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();
        long maxOffset = symbolTableEntries.get(symbolTableEntries.size() - 1).getLinkNameOffset().getInstance(Long.class);
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(maxOffset, hdfDataFile.getFixedPointDatatypeForOffset()));

        // --- Step 5: Split if SNOD too large ---
        if (symbolTableEntries.size() > MAX_SNOD_ENTRIES) {
            splitSnod(targetSnodIndex, fileAllocation, group);
        }
    }

    private int binarySearchDatasetName(
            int size,
            Function<Integer, Integer> compareNames
    ) {
        int low = 0;
        int high = size - 1;
        int insertionPoint = size;

        while (low <= high) {
            int mid = low + ((high - low) >>> 1);
            if (compareNames.apply(mid) <= 0) {
                insertionPoint = mid;
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return insertionPoint;
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
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(targetMaxLinkNameOffset, hdfDataFile.getFixedPointDatatypeForOffset()));

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
        HdfFixedPoint newKey = HdfWriteUtils.hdfFixedPointFromValue(newMaxLinkNameOffset, hdfDataFile.getFixedPointDatatypeForOffset());
        HdfFixedPoint newChildPointer = HdfWriteUtils.hdfFixedPointFromValue(newSnodOffset, hdfDataFile.getFixedPointDatatypeForOffset());
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
        long bTreeOffset = fileAllocation.getBtreeRecord().getOffset();
        seekableByteChannel.position(bTreeOffset);
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }

        Map<Long, HdfGroupSymbolTableNode> mapOffsetToSnod = mapOffsetToSnod();
        //TODO: hardcoed SNod storage size.
        ByteBuffer snodBuffer = ByteBuffer.allocate(328).order(ByteOrder.LITTLE_ENDIAN);
        for (Map.Entry<Long, HdfGroupSymbolTableNode> offsetAndStn : mapOffsetToSnod.entrySet()) {
            offsetAndStn.getValue().writeToBuffer(snodBuffer);
            snodBuffer.rewind();
            seekableByteChannel.position(offsetAndStn.getKey());
            while (snodBuffer.hasRemaining()) {
                seekableByteChannel.write(snodBuffer);
            }
            Arrays.fill(snodBuffer.array(), (byte) 0);
            snodBuffer.clear();        }
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