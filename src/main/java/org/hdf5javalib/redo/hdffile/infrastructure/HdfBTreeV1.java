package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.AllocationRecord;
import org.hdf5javalib.redo.AllocationType;
import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.HdfFileAllocation;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataSet;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfGroup;
import org.hdf5javalib.redo.utils.HdfReadUtils;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.*;
import java.util.function.Function;

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
            HdfDataFile hdfDataFile,
            String name, HdfFixedPoint offset
    ) {
        super(AllocationType.BTREE_HEADER, name, offset,
            new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset())
        );
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
            String name, HdfFixedPoint offset
    ) {
        super(AllocationType.BTREE_HEADER, name, offset,
                new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset())
        );
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.hdfDataFile = hdfDataFile;
        this.entriesUsed = 0;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength());
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
    public static HdfBTreeV1 readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
//            String groupName,
            HdfLocalHeap localHeap
    ) throws Exception {
        long initialAddress = fileChannel.position();
        return readFromSeekableByteChannelRecursive(fileChannel, initialAddress, hdfDataFile, localHeap, new LinkedHashMap<>());
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
                                                           HdfDataFile hdfDataFile,
                                                           HdfLocalHeap localHeap,
//                                                           String groupName,
                                                           Map<Long, HdfBTreeV1> visitedNodes
    ) throws Exception {
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        fileChannel.position(nodeAddress);
        long startPos = nodeAddress;

        int headerSize = 8 + hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().getSize() + hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().getSize();
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerBuffer);
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

        HdfFixedPoint leftSiblingAddress = HdfReadUtils.checkUndefined(headerBuffer, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().getSize()) ? hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().undefined(headerBuffer) : HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), headerBuffer);
        HdfFixedPoint rightSiblingAddress = HdfReadUtils.checkUndefined(headerBuffer, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().getSize()) ? hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().undefined(headerBuffer) : HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), headerBuffer);

        int entriesDataSize = hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength().getSize()
                + (entriesUsed * (hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().getSize() + hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength().getSize()));
        ByteBuffer entriesBuffer = ByteBuffer.allocate(entriesDataSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(entriesBuffer);
        entriesBuffer.flip();

        HdfFixedPoint keyZero = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength(), entriesBuffer);

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);

        HdfBTreeV1 currentNode = new HdfBTreeV1(signature, nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries, hdfDataFile,
                "Btree for ", HdfWriteUtils.hdfFixedPointFromValue(startPos, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()));
        visitedNodes.put(nodeAddress, currentNode);

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), entriesBuffer);
            HdfFixedPoint key = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength(), entriesBuffer);
            long filePosAfterEntriesBlock = fileChannel.position();
            long childAddress = childPointer.getInstance(Long.class);
//            HdfBTreeEntry entry;
//            if (nodeLevel == 0) {
//                if (childAddress != -1L) {
//                    fileChannel.position(childAddress);
//                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromSeekableByteChannel(fileChannel, hdfDataFile);
//                    entry = new HdfBTreeSnodEntry(key, childPointer, snod);
//                } else {
//                    HdfBTreeV1 childNode = readFromSeekableByteChannelRecursive(fileChannel, childAddress, hdfDataFile, visitedNodes);
//                    entry = new HdfBTreeChildBtreeEntry(key, childPointer, childNode);
//                }
//            } else {
//                if (childAddress != -1L) {
//                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromSeekableByteChannel(fileChannel, hdfDataFile);
//                    entry = new HdfBTreeSnodEntry(key, childPointer, snod);
//                } else {
//                    HdfBTreeV1 childNode = readFromSeekableByteChannelRecursive(fileChannel, childAddress, hdfDataFile, visitedNodes);
//                    entry = new HdfBTreeChildBtreeEntry(key, childPointer, childNode);
//                }
//            }

            fileChannel.position(childAddress);
            HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromSeekableByteChannel(fileChannel, hdfDataFile, localHeap);
            HdfBTreeEntry entry = new HdfBTreeSnodEntry(key, childPointer, snod);

            fileChannel.position(filePosAfterEntriesBlock);
            entries.add(entry);
        }
        return currentNode;
    }

    /**
     * Adds a dataset to the B-Tree, inserting it into the appropriate symbol table node.
     *
     * @param linkNameOffset           the offset of the link name in the local heap
     * @param dataset                   the dataset
     * @param group                    the parent group containing the dataset
     * @throws IllegalStateException if called on a non-leaf node
     * @throws IllegalArgumentException if the dataset name is null or empty
     */
    public void addDataset(
            long linkNameOffset,
            HdfDataSet dataset,
            HdfGroup group
    ) {
        if (!isLeafLevelNode()) {
            throw new IllegalStateException("addDataset can only be called on leaf B-tree nodes (nodeLevel 0).");
        }
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        final int MAX_SNOD_ENTRIES = 8;
        HdfGroupSymbolTableNode targetSnod;
        HdfBTreeEntry targetEntry;
        int targetSnodIndex;
        int insertIndex;

        // --- Step 1: Find or create target SNOD ---
        if (entries.isEmpty()) {
            HdfFixedPoint snodOffset = fileAllocation.allocateNextSnodStorage();
            targetSnod = new HdfGroupSymbolTableNode("SNOD",
                    1,
                    new ArrayList<>(MAX_SNOD_ENTRIES),
                    hdfDataFile,
                    group.getName()+":SNOD",
                    snodOffset
            );
            targetEntry = new HdfBTreeSnodEntry(
                    HdfWriteUtils.hdfFixedPointFromValue(linkNameOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                    snodOffset, targetSnod);
            entries.add(targetEntry);
            entriesUsed++;
            targetSnodIndex = 0;
            insertIndex = 0;
        } else {
            // instead of the full while loop over entries:
            targetSnodIndex = binarySearchDatasetName(entries.size(), (mid)->{
                HdfBTreeEntry entry = entries.get(mid);
                Long maxOffset = entry.getKey().getInstance(Long.class);
                String maxName = group.getDatasetNameByLinkNameOffset(maxOffset);
                return dataset.getDatasetName().compareTo(maxName);
            });
            targetSnodIndex = targetSnodIndex == entries.size() ? entries.size() - 1 : targetSnodIndex;

            targetEntry = entries.get(targetSnodIndex);
            targetSnod = ((HdfBTreeSnodEntry)targetEntry).getSymbolTableNode();

            // --- Step 2: Binary search within SNOD's symbol table ---
            insertIndex = binarySearchDatasetName(targetSnod.getSymbolTableEntries().size(), (mid)->{
                HdfSymbolTableEntry ste = targetSnod.getSymbolTableEntries().get(mid);
                Long offset = ste.getLinkNameOffset().getInstance(Long.class);
                String name = group.getDatasetNameByLinkNameOffset(offset);
                return dataset.getDatasetName().compareTo(name);
            });
        }

        // --- Step 3: Insert new dataset ---
//        HdfWriteUtils.hdfFixedPointFromValue(datasetObjectHeaderAddress, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset());
        HdfSymbolTableEntryCacheNotUsed steCache = new HdfSymbolTableEntryCacheNotUsed(hdfDataFile, dataset.getDataObjectHeaderPrefix(), dataset.getDatasetName());
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
                HdfWriteUtils.hdfFixedPointFromValue(linkNameOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                steCache
        );
        targetSnod.getSymbolTableEntries().add(insertIndex, ste);

        // --- Step 4: Update B-tree key with new max ---
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();
        long maxOffset = symbolTableEntries.get(symbolTableEntries.size() - 1).getLinkNameOffset().getInstance(Long.class);
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(maxOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()));

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
        HdfGroupSymbolTableNode targetSnod = ((HdfBTreeSnodEntry)targetEntry).getSymbolTableNode();
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();

        // Create new SNOD
        HdfFixedPoint newSnodOffset = fileAllocation.allocateNextSnodStorage();
        HdfGroupSymbolTableNode newSnod = new HdfGroupSymbolTableNode("SNOD",
                1,
                new ArrayList<>(MAX_SNOD_ENTRIES),
                hdfDataFile,
                group.getName()+":SNOD",
                newSnodOffset
        );

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
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(targetMaxLinkNameOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()));

        // Create new BTreeEntry
        String newMaxName = newSnodEntries.stream()
                .map(e -> group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class)))
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .orElseThrow(() -> new IllegalStateException("No valid dataset names in new SNOD"));
        HdfFixedPoint newMaxLinkNameOffset = newSnodEntries.stream()
                .filter(e -> newMaxName.equals(group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset().getInstance(Long.class))))
                .findFirst()
                .map(e -> e.getLinkNameOffset())
                .orElseThrow(() -> new IllegalStateException("Could not find linkNameOffset for max dataset name: " + newMaxName));
        HdfFixedPoint newKey = newMaxLinkNameOffset;
        HdfFixedPoint newChildPointer = newSnodOffset;
        HdfBTreeEntry newEntry = new HdfBTreeSnodEntry(newKey, newChildPointer, newSnod);

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
        ByteBuffer buffer = ByteBuffer.allocate(
                new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()).getInstance(Long.class).intValue())
                .order(ByteOrder.LITTLE_ENDIAN);
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
        long bTreeOffset = getOffset().getInstance(Long.class);
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
                HdfGroupSymbolTableNode snod = ((HdfBTreeSnodEntry)entry).getSymbolTableNode();
                if (snod != null) {
                    long offset = entry.getChildPointer().getInstance(Long.class);
                    if (offset != -1L) {
                        map.put(offset, snod);
                    }
                }
            }
        } else {
            for (HdfBTreeEntry entry : entries) {
                HdfBTreeV1 childBTree = ((HdfBTreeChildBtreeEntry)entry).getChildBTree();
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