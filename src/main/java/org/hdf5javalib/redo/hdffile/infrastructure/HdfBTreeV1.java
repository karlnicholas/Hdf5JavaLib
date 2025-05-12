package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.AllocationRecord;
import org.hdf5javalib.redo.AllocationType;
import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.HdfFileAllocation;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfGroup;
import org.hdf5javalib.redo.utils.HdfReadUtils;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.function.Function;

import static org.hdf5javalib.redo.HdfFileAllocation.BTREE_NODE_SIZE;
import static org.hdf5javalib.redo.HdfFileAllocation.BTREE_STORAGE_SIZE;
import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents an HDF5 B-Tree (version 1) as defined in the HDF5 specification.
 * <p>
 * The {@code HdfBTreeV1} class models a B-Tree used for indexing group entries in HDF5 files.
 * It supports both leaf nodes (containing symbol table nodes) and internal nodes (containing
 * child B-Trees). This class provides methods for reading from a file channel, adding datasets,
 * splitting symbol table nodes, and writing the B-Tree structure back to a file.
 * </p>
 *
 * @see HdfDataFile
 * @see HdfFixedPoint
 * @see HdfGroup
 * @see HdfBTreeEntry
 */
public class HdfBTreeV1 extends AllocationRecord {
    /** logger */
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HdfBTreeV1.class);
    /** The signature of the B-Tree node ("TREE"). */
    private final String signature;
    /** The type of the node (0 for group B-Tree). */
    private final int nodeType;
    /** The level of the node (0 for leaf, >0 for internal). */
    private final int nodeLevel;
    /** The number of entries used in the node. */
    private int entriesUsed;
    /** The address of the left sibling node. */
    private final HdfFixedPoint leftSiblingAddress;
    /** The address of the right sibling node. */
    private final HdfFixedPoint rightSiblingAddress;
    /** The first key (key zero) of the node. */
    private final HdfFixedPoint keyZero;
    /** The list of B-Tree entries. */
    private final List<HdfBTreeEntry> entries;
    /** The HDF5 file context. */
    private final HdfDataFile hdfDataFile;

    /**
     * Constructs an HdfBTreeV1 with all fields specified.
     *
     * @param signature         the signature of the B-Tree node ("TREE")
     * @param nodeType          the type of the node (0 for group B-Tree)
     * @param nodeLevel         the level of the node (0 for leaf, >0 for internal)
     * @param entriesUsed       the number of entries used in the node
     * @param leftSiblingAddress the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     * @param keyZero           the first key of the node
     * @param entries           the list of B-Tree entries
     * @param hdfDataFile       the HDF5 file context
     */
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
        super(AllocationType.BTREE_HEADER, "btree", HdfFileAllocation.B)
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

    /**
     * Constructs an HdfBTreeV1 with minimal fields for a new node.
     *
     * @param signature         the signature of the B-Tree node ("TREE")
     * @param nodeType          the type of the node (0 for group B-Tree)
     * @param nodeLevel         the level of the node (0 for leaf, >0 for internal)
     * @param leftSiblingAddress the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     * @param hdfDataFile       the HDF5 file context
     */
    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfDataFile hdfDataFile,
            String name,
            HdfFixedPoint offset
    ) {
        super(AllocationType.BTREE_HEADER, name, offset, BTREE_NODE_SIZE + BTREE_STORAGE_SIZE);
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.hdfDataFile = hdfDataFile;
        this.entriesUsed = 0;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength());
        this.entries = new ArrayList<>();
    }

    /**
     * Reads an HdfBTreeV1 from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfBTreeV1 instance
     * @throws IOException if an I/O error occurs or the B-Tree data is invalid
     */
    public static HdfBTreeV1 readFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws Exception {
        long initialAddress = fileChannel.position();
        return readFromSeekableByteChannelRecursive(fileChannel, initialAddress, new HashMap<>(), hdfDataFile);
    }

    /**
     * Recursively reads an HdfBTreeV1 from a file channel, handling cycles.
     *
     * @param fileChannel the file channel to read from
     * @param nodeAddress the address of the current node
     * @param visitedNodes a map of visited node addresses to detect cycles
     * @param hdfDataFile  the HDF5 file context
     * @return the constructed HdfBTreeV1 instance
     * @throws IOException if an I/O error occurs or the B-Tree data is invalid
     */
    private static HdfBTreeV1 readFromSeekableByteChannelRecursive(SeekableByteChannel fileChannel,
                                                           long nodeAddress,
                                                           Map<Long, HdfBTreeV1> visitedNodes,
                                                           HdfDataFile hdfDataFile
    ) throws Exception {
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        fileChannel.position(nodeAddress);
        long startPos = nodeAddress;

        int headerSize = 8 + hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().getSize() + hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().getSize();
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
        HdfFixedPoint leftSiblingAddress = HdfReadUtils.checkUndefined(headerBuffer, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().getSize()) ? hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().undefined(headerBuffer) : HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), headerBuffer);
        HdfFixedPoint rightSiblingAddress = HdfReadUtils.checkUndefined(headerBuffer, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().getSize()) ? hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().undefined(headerBuffer) : HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), headerBuffer);

        int entriesDataSize = hdfDataFile.getSuperblock().getFixedPointDatatypeForLength().getSize()
                + (entriesUsed * (hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset().getSize() + hdfDataFile.getSuperblock().getFixedPointDatatypeForLength().getSize()));
        if (entriesUsed < 0 || entriesDataSize < hdfDataFile.getSuperblock().getFixedPointDatatypeForLength().getSize()) {
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

        HdfFixedPoint keyZero = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), entriesBuffer);

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);
        long filePosAfterEntriesBlock = fileChannel.position();

        HdfBTreeV1 currentNode = new HdfBTreeV1(signature, nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries, hdfDataFile);
        visitedNodes.put(nodeAddress, currentNode);

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), entriesBuffer);
            HdfFixedPoint key = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), entriesBuffer);
            long childAddress = childPointer.getInstance(Long.class);
            HdfBTreeEntry entry;

            if (childAddress < -1L || (childAddress != -1L && childAddress >= fileSize)) {
                throw new IOException("Invalid child address " + childAddress + " in BTree entry " + i
                        + " at node " + startPos + " (Level " + nodeLevel + "). File size is " + fileSize);
            }

            if (nodeLevel == 0) {
                if (childAddress != -1L) {
                    fileChannel.position(childAddress);
                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromSeekableByteChannel(fileChannel, hdfDataFile);
                    entry = new HdfBTreeEntry(key, childPointer, snod);
                    fileChannel.position(filePosAfterEntriesBlock);
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfGroupSymbolTableNode) null);
                }
            } else {
                if (childAddress != -1L) {
                    HdfBTreeV1 childNode = readFromSeekableByteChannelRecursive(fileChannel, childAddress, visitedNodes, hdfDataFile);
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

    /**
     * Adds a dataset to the B-Tree, inserting it into the appropriate symbol table node.
     *
     * @param linkNameOffset           the offset of the link name in the local heap
     * @param datasetObjectHeaderAddress the address of the dataset's object header
     * @param datasetName              the name of the dataset
     * @param group                    the parent group containing the dataset
     * @throws IllegalStateException if called on a non-leaf node
     * @throws IllegalArgumentException if the dataset name is null or empty
     */
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
                    HdfWriteUtils.hdfFixedPointFromValue(linkNameOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()),
                    HdfWriteUtils.hdfFixedPointFromValue(snodOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()), targetSnod);
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
                HdfWriteUtils.hdfFixedPointFromValue(linkNameOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()),
                HdfWriteUtils.hdfFixedPointFromValue(datasetObjectHeaderAddress, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset())
        );
        targetSnod.getSymbolTableEntries().add(insertIndex, ste);

        // --- Step 4: Update B-tree key with new max ---
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();
        long maxOffset = symbolTableEntries.get(symbolTableEntries.size() - 1).getLinkNameOffset().getInstance(Long.class);
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(maxOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()));

        // --- Step 5: Split if SNOD too large ---
        if (symbolTableEntries.size() > MAX_SNOD_ENTRIES) {
            splitSnod(targetSnodIndex, fileAllocation, group);
        }
    }

    /**
     * Performs a binary search to find the insertion point for a dataset name.
     *
     * @param size        the size of the list to search
     * @param compareNames a function to compare dataset names
     * @return the insertion point index
     */
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

    /**
     * Splits a symbol table node if it exceeds the maximum entry limit.
     *
     * @param targetEntryIndex the index of the entry to split
     * @param fileAllocation   the file allocation manager
     * @param group            the parent group
     */
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
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(targetMaxLinkNameOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()));

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
        HdfFixedPoint newKey = HdfWriteUtils.hdfFixedPointFromValue(newMaxLinkNameOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset());
        HdfFixedPoint newChildPointer = HdfWriteUtils.hdfFixedPointFromValue(newSnodOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset());
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

    /**
     * Checks if this is a leaf-level node (nodeLevel == 0).
     *
     * @return true if the node is a leaf node, false otherwise
     */
    public boolean isLeafLevelNode() {
        return this.nodeLevel == 0;
    }

    /**
     * Checks if this is an internal-level node (nodeLevel > 0).
     *
     * @return true if the node is an internal node, false otherwise
     */
    public boolean isInternalLevelNode() {
        return this.nodeLevel > 0;
    }

    /**
     * Writes the B-Tree and its symbol table nodes to a file channel.
     *
     * @param seekableByteChannel the file channel to write to
     * @param fileAllocation      the file allocation manager
     * @throws IOException if an I/O error occurs
     */
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
            snodBuffer.clear();
        }
    }

    /**
     * Maps offsets to symbol table nodes in the B-Tree.
     *
     * @return a map of offsets to symbol table nodes
     */
    public Map<Long, HdfGroupSymbolTableNode> mapOffsetToSnod() {
        Map<Long, HdfGroupSymbolTableNode> offsetToSnodMap = new HashMap<>();
        collectSnodsRecursively(this, offsetToSnodMap);
        return offsetToSnodMap;
    }

    /**
     * Recursively collects symbol table nodes and their offsets.
     *
     * @param node the current B-Tree node
     * @param map  the map to store offset-to-SNOD mappings
     */
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

    /**
     * Returns a string representation of the HdfBTreeV1.
     *
     * @return a string describing the node's signature, type, level, entries, and structure
     */
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

    public List<HdfBTreeEntry> getEntries() {
        return entries;
    }

}