package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.datatype.FixedPointDatatype;
import org.hdf5javalib.redo.hdffile.AllocationRecord;
import org.hdf5javalib.redo.hdffile.AllocationType;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.HdfFileAllocation;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataObject;
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
    private static final byte[] BTREE_SIGNATURE = {'T', 'R', 'E', 'E'};
    private static final int BTREE_HEADER_INITIAL_SIZE = 8;
    private static final int MAX_SNOD_ENTRIES = 8;
    /**
     * The type of the node (0 for group B-Tree).
     */
    private final int nodeType;
    /**
     * The level of the node (0 for leaf, >0 for internal).
     */
    private final int nodeLevel;
    /**
     * The number of entries used in the node.
     */
    private int entriesUsed;
    /**
     * The address of the left sibling node.
     */
    private final HdfFixedPoint leftSiblingAddress;
    /**
     * The address of the right sibling node.
     */
    private final HdfFixedPoint rightSiblingAddress;
    /**
     * The first key (key zero) of the node.
     */
    private final HdfFixedPoint keyZero;
    /**
     * The list of B-Tree entries.
     */
    private final List<HdfBTreeEntry> entries;
    /**
     * The HDF5 file context.
     */
    private final HdfDataFile hdfDataFile;

    /**
     * Constructs an HdfBTreeV1 with all fields specified.
     *
     * @param nodeType            the type of the node (0 for group B-Tree)
     * @param nodeLevel           the level of the node (0 for leaf, >0 for internal)
     * @param entriesUsed         the number of entries used in the node
     * @param leftSiblingAddress  the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     * @param keyZero             the first key of the node
     * @param entries             the list of B-Tree entries
     * @param hdfDataFile         the HDF5 file context
     */
    public HdfBTreeV1(
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
                new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                hdfDataFile.getFileAllocation()
        );
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
     * @param nodeType            the type of the node (0 for group B-Tree)
     * @param nodeLevel           the level of the node (0 for leaf, >0 for internal)
     * @param leftSiblingAddress  the address of the left sibling node
     * @param rightSiblingAddress the address of the right sibling node
     * @param hdfDataFile         the HDF5 file context
     */
    public HdfBTreeV1(
            int nodeType,
            int nodeLevel,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfDataFile hdfDataFile,
            String name, HdfFixedPoint offset
    ) {
        super(AllocationType.BTREE_HEADER, name, offset,
                new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                hdfDataFile.getFileAllocation()
        );
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
            HdfLocalHeap localHeap,
            String objectName
    ) throws Exception {
        long initialAddress = fileChannel.position();
        return readFromSeekableByteChannelRecursive(fileChannel, initialAddress, hdfDataFile, localHeap, objectName, new LinkedHashMap<>());
    }

    /**
     * Recursively reads an HdfBTreeV1 from a file channel, handling cycles.
     *
     * @param fileChannel  the file channel to read from
     * @param nodeAddress  the address of the current node
     * @param visitedNodes a map of visited node addresses to detect cycles
     * @param hdfDataFile  the HDF5 file context
     * @return the constructed HdfBTreeV1 instance
     * @throws IOException if an I/O error occurs or the B-Tree data is invalid
     */
    private static HdfBTreeV1 readFromSeekableByteChannelRecursive(SeekableByteChannel fileChannel,
                                                                   long nodeAddress,
                                                                   HdfDataFile hdfDataFile,
                                                                   HdfLocalHeap localHeap,
                                                                   String objectName,
                                                                   Map<Long, HdfBTreeV1> visitedNodes
    ) throws Exception {
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        fileChannel.position(nodeAddress);
        FixedPointDatatype hdfOffset = hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset();
        final int offsetSize = hdfOffset.getSize();
        FixedPointDatatype hdfLength = hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset();
        final int lengthSize = hdfLength.getSize();

        int headerSize = BTREE_HEADER_INITIAL_SIZE + offsetSize + offsetSize;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerBuffer);
        headerBuffer.flip();

        byte[] signatureBytes = new byte[BTREE_SIGNATURE.length];
        headerBuffer.get(signatureBytes);
        if (Arrays.compare(signatureBytes, BTREE_SIGNATURE) != 0) {
            throw new IOException("Invalid B-tree node signature: '" + Arrays.toString(signatureBytes) + "' at position " + nodeAddress);
        }

        int nodeType = Byte.toUnsignedInt(headerBuffer.get());
        int nodeLevel = Byte.toUnsignedInt(headerBuffer.get());
        int entriesUsed = Short.toUnsignedInt(headerBuffer.getShort());

        HdfFixedPoint leftSiblingAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfOffset, headerBuffer);
        HdfFixedPoint rightSiblingAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfOffset, headerBuffer);

        int entriesDataSize = lengthSize + (entriesUsed * (offsetSize + lengthSize));
        ByteBuffer entriesBuffer = ByteBuffer.allocate(entriesDataSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(entriesBuffer);
        entriesBuffer.flip();

        HdfFixedPoint keyZero = HdfReadUtils.readHdfFixedPointFromBuffer(hdfLength, entriesBuffer);

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);

        HdfBTreeV1 currentNode = new HdfBTreeV1(nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries, hdfDataFile,
                objectName + ":Btree", HdfWriteUtils.hdfFixedPointFromValue(nodeAddress, hdfOffset));
        visitedNodes.put(nodeAddress, currentNode);

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfReadUtils.readHdfFixedPointFromBuffer(hdfOffset, entriesBuffer);
            HdfFixedPoint key = HdfReadUtils.readHdfFixedPointFromBuffer(hdfLength, entriesBuffer);
            long filePosAfterEntriesBlock = fileChannel.position();
            long childAddress = childPointer.getInstance(Long.class);
            fileChannel.position(childAddress);
            HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromSeekableByteChannel(fileChannel, hdfDataFile, localHeap, objectName);
            HdfBTreeEntry entry = new HdfBTreeSnodEntry(key, childPointer, snod);

            fileChannel.position(filePosAfterEntriesBlock);
            entries.add(entry);
        }
        return currentNode;
    }

    /**
     * Adds a dataset to the B-tree, inserting it into the appropriate symbol table node.
     *
     * @param linkNameOffset the offset of the link name in the local heap
     * @param dataset        the dataset
     * @param group          the parent group containing the dataset
     * @throws IllegalArgumentException if dataset name or linkNameOffset is null or empty
     * @throws IllegalStateException    if called on a non-leaf node or node structure is invalid
     */
    public void addDataset(HdfFixedPoint linkNameOffset, HdfDataSet dataset, HdfGroup group) {
        if (dataset == null || dataset.getDatasetName() == null || dataset.getDatasetName().isEmpty()) {
            throw new IllegalArgumentException("Dataset or dataset name cannot be null or empty");
        }
        if (linkNameOffset == null) {
            throw new IllegalArgumentException("Link name offset cannot be null");
        }
        if (nodeLevel != 0) {
            throw new IllegalStateException("addDataset called on non-leaf node (level: " + nodeLevel + ")");
        }
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        HdfGroupSymbolTableNode targetSnod;
        HdfBTreeEntry targetEntry;
        int targetSnodIndex;
        int insertIndex;

        // Step 1: Find or create target SNOD
        if (entries.isEmpty()) {
            HdfFixedPoint snodOffset = fileAllocation.allocateNextSnodStorage();
            targetSnod = new HdfGroupSymbolTableNode(1, new ArrayList<>(MAX_SNOD_ENTRIES), hdfDataFile,
                    group.getGroupName() + ":SNOD", snodOffset);
            targetEntry = new HdfBTreeSnodEntry(linkNameOffset, snodOffset, targetSnod);
            entries.add(targetEntry);
            entriesUsed++;
            targetSnodIndex = 0;
            insertIndex = 0;
        } else {
            Optional<SnodSearchResult> searchResult = findSnodAndInsertIndex(dataset.getDatasetName(), group);
            if (searchResult.isEmpty()) {
                // No suitable SNOD found, use the last one
                targetSnodIndex = entries.size() - 1;
                insertIndex = ((HdfBTreeSnodEntry) entries.get(targetSnodIndex)).getSymbolTableNode().getSymbolTableEntries().size();
            } else {
                SnodSearchResult result = searchResult.get();
                targetSnodIndex = result.entryIndex;
                insertIndex = result.insertIndex;
            }
            targetEntry = entries.get(targetSnodIndex);
            if (!(targetEntry instanceof HdfBTreeSnodEntry)) {
                throw new IllegalStateException("Expected HdfBTreeSnodEntry at index " + targetSnodIndex + ", found: " + targetEntry.getClass().getName());
            }
            targetSnod = ((HdfBTreeSnodEntry) targetEntry).getSymbolTableNode();
        }

        // Step 2: Insert new dataset
        HdfSymbolTableEntryCacheNotUsed steCache = new HdfSymbolTableEntryCacheNotUsed(hdfDataFile, dataset.getDataObjectHeaderPrefix(), dataset.getDatasetName());
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(linkNameOffset, steCache);
        targetSnod.getSymbolTableEntries().add(insertIndex, ste);

        // Step 3: Update B-tree key with new max
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();
        long maxOffset = symbolTableEntries.get(symbolTableEntries.size() - 1).getLinkNameOffset().getInstance(Long.class);
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(maxOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()));

        // Step 4: Split if SNOD too large
        if (symbolTableEntries.size() > MAX_SNOD_ENTRIES) {
            splitSnod(targetSnodIndex, fileAllocation, group);
        }
    }

    /**
     * Helper class to hold binary search results for SNOD and insertion index.
     */
    private static class SnodSearchResult {
        final int entryIndex;
        final int insertIndex;

        SnodSearchResult(int entryIndex, int insertIndex) {
            this.entryIndex = entryIndex;
            this.insertIndex = insertIndex;
        }
    }

    /**
     * Finds the HdfBTreeSnodEntry and insertion index for a given object name using binary search.
     *
     * @param name  the object name to search for
     * @param group the parent group for resolving link name offsets
     * @return an Optional containing the SnodSearchResult with entry index and insertion index, or Optional.empty()
     * @throws IllegalStateException if name resolution fails
     */
    private Optional<SnodSearchResult> findSnodAndInsertIndex(String name, HdfGroup group) {
        int entryIndex = binarySearchDatasetName(entries.size(), (mid) -> {
            HdfFixedPoint maxOffset = entries.get(mid).getKey();
            String maxName = group.getDatasetNameByLinkNameOffset(maxOffset);
            if (maxName == null) {
                throw new IllegalStateException("Null dataset name for linkNameOffset: " + maxOffset);
            }
            return name.compareTo(maxName);
        });
        if (entryIndex >= entries.size()) {
            return Optional.empty();
        }
        if (!(entries.get(entryIndex) instanceof HdfBTreeSnodEntry snodEntry)) {
            throw new IllegalStateException("Expected HdfBTreeSnodEntry at index " + entryIndex + ", found: " + entries.get(entryIndex).getClass().getName());
        }
        HdfGroupSymbolTableNode snod = snodEntry.getSymbolTableNode();
        if (snod == null || snod.getSymbolTableEntries() == null || snod.getSymbolTableEntries().isEmpty()) {
            return Optional.of(new SnodSearchResult(entryIndex, 0));
        }
        int insertIndex = binarySearchDatasetName(snod.getSymbolTableEntries().size(), (mid) -> {
            HdfSymbolTableEntry ste = snod.getSymbolTableEntries().get(mid);
            HdfFixedPoint offset = ste.getLinkNameOffset();
            String steName = group.getDatasetNameByLinkNameOffset(offset);
            if (steName == null) {
                throw new IllegalStateException("Null dataset name for linkNameOffset: " + offset);
            }
            return name.compareTo(steName);
        });
        return Optional.of(new SnodSearchResult(entryIndex, insertIndex));
    }

    /**
     * Finds an HdfDataObject by name in the B-tree using binary search.
     *
     * @param name  the object name to find (dataset or group)
     * @param group the parent group for resolving link name offsets
     * @return an Optional containing the HdfDataObject if found, or Optional.empty()
     * @throws IllegalArgumentException if name is null or empty
     * @throws IllegalStateException    if name resolution fails or node structure is invalid
     */
    public Optional<HdfDataObject> findObjectByName(String name, HdfGroup group) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Object name cannot be null or empty");
        }
        if (nodeLevel == 0) {
            // Leaf node: find HdfBTreeSnodEntry and HdfSymbolTableEntry
            Optional<HdfSymbolTableEntry> ste = findSymbolTableEntry(name, group);
            if (ste.isEmpty()) {
                return Optional.empty();
            }
            HdfSymbolTableEntry entry = ste.get();
            if (entry.getCache().getCacheType() == 0) {
                if (!(entry.getCache() instanceof HdfSymbolTableEntryCacheNotUsed)) {
                    throw new IllegalStateException("Expected HdfSymbolTableEntryCacheNotUsed for cacheType 0, found: " + entry.getCache().getClass().getName());
                }
                return Optional.of(((HdfSymbolTableEntryCacheNotUsed) entry.getCache()).getDataSet());
            } else if (entry.getCache().getCacheType() == 1) {
                if (!(entry.getCache() instanceof HdfSymbolTableEntryCacheGroupMetadata)) {
                    throw new IllegalStateException("Expected HdfSymbolTableEntryCacheGroupMetadata for cacheType 1, found: " + entry.getCache().getClass().getName());
                }
                return Optional.of(((HdfSymbolTableEntryCacheGroupMetadata) entry.getCache()).getGroup());
            } else {
                throw new IllegalStateException("Unexpected cacheType: " + entry.getCache().getCacheType());
            }
        } else {
            // Internal node: binary search for HdfBTreeChildBtreeEntry
            int entryIndex = binarySearchDatasetName(entries.size(), (mid) -> {
                HdfFixedPoint maxOffset = entries.get(mid).getKey();
                String maxName = group.getDatasetNameByLinkNameOffset(maxOffset);
                if (maxName == null) {
                    throw new IllegalStateException("Null dataset name for linkNameOffset: " + maxOffset);
                }
                String minName = mid == 0 ? "" : group.getDatasetNameByLinkNameOffset(entries.get(mid - 1).getKey());
                if (minName == null) {
                    throw new IllegalStateException("Null dataset name for previous linkNameOffset: " + entries.get(mid - 1).getKey());
                }
                if (name.compareTo(minName) > 0 && name.compareTo(maxName) <= 0) {
                    return 0; // Found the range
                }
                return name.compareTo(maxName);
            });
            if (entryIndex >= entries.size()) {
                return Optional.empty();
            }
            if (!(entries.get(entryIndex) instanceof HdfBTreeChildBtreeEntry childBtreeEntry)) {
                throw new IllegalStateException("Expected HdfBTreeChildBtreeEntry at index " + entryIndex + ", found: " + entries.get(entryIndex).getClass().getName());
            }
            HdfBTreeV1 childBTree = childBtreeEntry.getChildBTree();
            if (childBTree == null) {
                throw new IllegalStateException("Null child B-tree for entry at index " + entryIndex);
            }
            return childBTree.findObjectByName(name, group);
        }
    }

    /**
     * Performs a binary search to find the insertion point for a dataset name.
     *
     * @param size         the size of the list to search
     * @param compareNames a function to compare dataset names
     * @return the insertion point index
     */
    private static int binarySearchDatasetName(
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
        HdfGroupSymbolTableNode targetSnod = ((HdfBTreeSnodEntry) targetEntry).getSymbolTableNode();
        List<HdfSymbolTableEntry> symbolTableEntries = targetSnod.getSymbolTableEntries();

        // Create new SNOD
        HdfFixedPoint newSnodOffset = fileAllocation.allocateNextSnodStorage();
        HdfGroupSymbolTableNode newSnod = new HdfGroupSymbolTableNode(
                1,
                new ArrayList<>(MAX_SNOD_ENTRIES),
                hdfDataFile,
                group.getGroupName() + ":SNOD",
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
                .map(e -> group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset()))
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .orElseThrow(() -> new IllegalStateException("No valid dataset names in target SNOD"));
        long targetMaxLinkNameOffset = symbolTableEntries.stream()
                .filter(e -> targetMaxName.equals(group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset())))
                .findFirst()
                .map(e -> e.getLinkNameOffset().getInstance(Long.class))
                .orElseThrow(() -> new IllegalStateException("Could not find linkNameOffset for max dataset name: " + targetMaxName));
        targetEntry.setKey(HdfWriteUtils.hdfFixedPointFromValue(targetMaxLinkNameOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()));

        // Create new BTreeEntry
        String newMaxName = newSnodEntries.stream()
                .map(e -> group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset()))
                .filter(Objects::nonNull)
                .max(String::compareTo)
                .orElseThrow(() -> new IllegalStateException("No valid dataset names in new SNOD"));
        HdfFixedPoint newKey = newSnodEntries.stream()
                .filter(e -> newMaxName.equals(group.getDatasetNameByLinkNameOffset(e.getLinkNameOffset())))
                .findFirst()
                .map(HdfSymbolTableEntry::getLinkNameOffset)
                .orElseThrow(() -> new IllegalStateException("Could not find linkNameOffset for max dataset name: " + newMaxName));
        HdfBTreeEntry newEntry = new HdfBTreeSnodEntry(newKey, newSnodOffset, newSnod);

        // Insert new BTreeEntry in sorted order
        int insertPos = targetEntryIndex + 1;
        for (int i = targetEntryIndex + 1; i < entries.size(); i++) {
            String maxName = group.getDatasetNameByLinkNameOffset(entries.get(i).getKey());
            if (maxName == null) {
                throw new IllegalStateException("No dataset name found for key linkNameOffset: " + entries.get(i).getKey());
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
     * Writes the B-Tree and its symbol table nodes to a file channel.
     *
     * @param seekableByteChannel the file channel to write to
     * @param ignoredFileAllocation      the file allocation manager
     * @throws IOException if an I/O error occurs
     */
    public void writeToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation ignoredFileAllocation) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(
                        new HdfFixedPoint(hdfDataFile.getFileAllocation().HDF_BTREE_NODE_SIZE.add(hdfDataFile.getFileAllocation().HDF_BTREE_STORAGE_SIZE), hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()).getInstance(Long.class).intValue())
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(BTREE_SIGNATURE);
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

        // Leaf node
        if (node.nodeLevel == 0) {
            for (HdfBTreeEntry entry : entries) {
                HdfGroupSymbolTableNode snod = ((HdfBTreeSnodEntry) entry).getSymbolTableNode();
                if (snod != null) {
                    long offset = entry.getChildPointer().getInstance(Long.class);
                    if (offset != -1L) {
                        map.put(offset, snod);
                    }
                }
            }
        } else {
            for (HdfBTreeEntry entry : entries) {
                HdfBTreeV1 childBTree = ((HdfBTreeChildBtreeEntry) entry).getChildBTree();
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
        sb.append("signature='").append(Arrays.toString(BTREE_SIGNATURE)).append('\'');
        sb.append(", nodeType=").append(nodeType);
        sb.append(", nodeLevel=").append(nodeLevel);
        sb.append(", entriesUsed=").append(entriesUsed);
        sb.append(", leftSiblingAddress=").append(leftSiblingAddress.isUndefined() ? "undefined" : leftSiblingAddress);
        sb.append(", rightSiblingAddress=").append(rightSiblingAddress.isUndefined() ? "undefined" : rightSiblingAddress);
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
    /**
     * Finds the full path to an HdfDataObject by name in the B-tree, returning a Deque of parent objects.
     *
     * @param name  the object name to find (dataset or group)
     * @param group the parent group for resolving link name offsets
     * @return an Optional containing a Deque of HdfDataObjects representing the path from the root group to the found object, or Optional.empty() if not found
     * @throws IllegalArgumentException if name is null or empty
     * @throws IllegalStateException    if name resolution fails or node structure is invalid
     */
    public Optional<Deque<HdfDataObject>> findObjectPathByName(String name, HdfGroup group) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Object name cannot be null or empty");
        }
        Deque<HdfDataObject> path = new ArrayDeque<>();
        path.push(group); // Start with the root group
        if (nodeLevel == 0) {
            // Leaf node: find HdfSymbolTableEntry and build path
            Optional<SymbolTableEntryWithPath> steWithPath = findSymbolTableEntryWithPath(name, group, path);
            if (steWithPath.isEmpty()) {
                return Optional.empty();
            }
            HdfSymbolTableEntry entry = steWithPath.get().entry;
            if (entry.getCache().getCacheType() == 0) {
                if (!(entry.getCache() instanceof HdfSymbolTableEntryCacheNotUsed)) {
                    throw new IllegalStateException("Expected HdfSymbolTableEntryCacheNotUsed for cacheType 0, found: " + entry.getCache().getClass().getName());
                }
                path.push(((HdfSymbolTableEntryCacheNotUsed) entry.getCache()).getDataSet());
                return Optional.of(path);
            } else if (entry.getCache().getCacheType() == 1) {
                if (!(entry.getCache() instanceof HdfSymbolTableEntryCacheGroupMetadata)) {
                    throw new IllegalStateException("Expected HdfSymbolTableEntryCacheGroupMetadata for cacheType 1, found: " + entry.getCache().getClass().getName());
                }
                path.push(((HdfSymbolTableEntryCacheGroupMetadata) entry.getCache()).getGroup());
                return Optional.of(path);
            } else {
                throw new IllegalStateException("Unexpected cacheType: " + entry.getCache().getCacheType());
            }
        } else {
            // Internal node: binary search for HdfBTreeChildBtreeEntry
            int entryIndex = binarySearchDatasetName(entries.size(), (mid) -> {
                HdfFixedPoint maxOffset = entries.get(mid).getKey();
                String maxName = group.getDatasetNameByLinkNameOffset(maxOffset);
                if (maxName == null) {
                    throw new IllegalStateException("Null dataset name for linkNameOffset: " + maxOffset);
                }
                String minName = mid == 0 ? "" : group.getDatasetNameByLinkNameOffset(entries.get(mid - 1).getKey());
                if (minName == null) {
                    throw new IllegalStateException("Null dataset name for previous linkNameOffset: " + entries.get(mid - 1).getKey());
                }
                if (name.compareTo(minName) > 0 && name.compareTo(maxName) <= 0) {
                    return 0; // Found the range
                }
                return name.compareTo(maxName);
            });
            if (entryIndex >= entries.size()) {
                return Optional.empty();
            }
            if (!(entries.get(entryIndex) instanceof HdfBTreeChildBtreeEntry childBtreeEntry)) {
                throw new IllegalStateException("Expected HdfBTreeChildBtreeEntry at index " + entryIndex + ", found: " + entries.get(entryIndex).getClass().getName());
            }
            HdfBTreeV1 childBTree = childBtreeEntry.getChildBTree();
            if (childBTree == null) {
                throw new IllegalStateException("Null child B-tree for entry at index " + entryIndex);
            }
            return childBTree.findObjectPathByName(name, group).map(childPath -> {
                // Merge paths: parent path + child path
                Deque<HdfDataObject> fullPath = new ArrayDeque<>(path);
                fullPath.addAll(childPath);
                return fullPath;
            });
        }
    }

    /**
     * Helper class to hold HdfSymbolTableEntry and its path.
     */
    private static class SymbolTableEntryWithPath {
        final HdfSymbolTableEntry entry;
        final Deque<HdfDataObject> path;

        SymbolTableEntryWithPath(HdfSymbolTableEntry entry, Deque<HdfDataObject> path) {
            this.entry = entry;
            this.path = path;
        }
    }

    /**
     * Finds the HdfSymbolTableEntry for a given object name using binary search, optionally tracking the path.
     *
     * @param name  the object name to search for
     * @param group the parent group for resolving link name offsets
     * @param path  the Deque to store the path (may be null if path tracking is not needed)
     * @return an Optional containing the SymbolTableEntryWithPath if found, or Optional.empty()
     * @throws IllegalStateException if name resolution fails or node structure is invalid
     */
    private Optional<SymbolTableEntryWithPath> findSymbolTableEntryWithPath(String name, HdfGroup group, Deque<HdfDataObject> path) {
        Optional<SnodSearchResult> searchResult = findSnodAndInsertIndex(name, group);
        if (searchResult.isEmpty()) {
            return Optional.empty();
        }
        SnodSearchResult result = searchResult.get();
        int entryIndex = result.entryIndex;
        int steIndex = result.insertIndex;
        HdfBTreeSnodEntry snodEntry = (HdfBTreeSnodEntry) entries.get(entryIndex);
        HdfGroupSymbolTableNode snod = snodEntry.getSymbolTableNode();
        if (steIndex >= snod.getSymbolTableEntries().size()) {
            return Optional.empty();
        }
        HdfSymbolTableEntry ste = snod.getSymbolTableEntries().get(steIndex);
        String steName = group.getDatasetNameByLinkNameOffset(ste.getLinkNameOffset());
        if (!name.equals(steName)) {
            return Optional.empty();
        }
        return Optional.of(new SymbolTableEntryWithPath(ste, path));
    }

    /**
     * Finds the HdfSymbolTableEntry for a given object name using binary search.
     *
     * @param name  the object name to search for
     * @param group the parent group for resolving link name offsets
     * @return an Optional containing the HdfSymbolTableEntry if found, or Optional.empty()
     * @throws IllegalStateException if name resolution fails or node structure is invalid
     */
    private Optional<HdfSymbolTableEntry> findSymbolTableEntry(String name, HdfGroup group) {
        return findSymbolTableEntryWithPath(name, group, null).map(steWithPath -> steWithPath.entry);
    }
}