package org.hdf5javalib;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.*;
import org.hdf5javalib.file.metadata.HdfSuperblock;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Getter
@Slf4j
public class HdfFileReader implements HdfDataFile {
    // level 0
    private HdfSuperblock superblock;
    // level 1
    private HdfGroup rootGroup;

    private final SeekableByteChannel fileChannel; // Changed from FileChannel
    private final HdfGlobalHeap globalHeap;
    private final HdfFileAllocation fileAllocation;

    public HdfFileReader(SeekableByteChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.fileAllocation = new HdfFileAllocation();
        this.globalHeap = new HdfGlobalHeap(this::initializeGlobalHeap, this);
    }

    private void initializeGlobalHeap(long offset) {
        try {
            fileChannel.position(offset);
            globalHeap.readFromFileChannel(fileChannel, (short)8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HdfFileReader readFile() throws IOException {
        // Parse the superblock at the beginning of the file
        superblock = HdfSuperblock.readFromFileChannel(fileChannel, this);
        log.debug("{}", superblock);

        short offsetSize = superblock.getOffsetSize();
        short lengthSize = superblock.getLengthSize();

        // Get the object header address from the superblock
        // Parse the object header from the file using the superblock information
        long objectHeaderAddress = superblock.getRootGroupSymbolTableEntry().getObjectHeaderOffset().getInstance(Long.class);
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize, this);

        // Parse the local heap using the file channel
        long localHeapAddress = superblock.getRootGroupSymbolTableEntry().getLocalHeapOffset().getInstance(Long.class);
        fileChannel.position(localHeapAddress);
        HdfLocalHeap localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize(), this);

        long dataSize = localHeap.getHeapContentsSize().getInstance(Long.class);
        long dataSegmentAddress = localHeap.getHeapContentsOffset().getInstance(Long.class);
        fileChannel.position(dataSegmentAddress);
        HdfLocalHeapContents localHeapContents = HdfLocalHeapContents.readFromFileChannel(fileChannel, (int) dataSize, this);

        long bTreeAddress = superblock.getRootGroupSymbolTableEntry().getBTreeOffset().getInstance(Long.class);
        fileChannel.position(bTreeAddress);
        HdfBTreeV1 bTree = HdfBTreeV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize(), this);

        rootGroup = new HdfGroup(
                null,
                "",
                objectHeader,
                bTree,
                localHeap,
                localHeapContents
        );

        log.debug("{}", rootGroup);

        log.debug("Parsing complete. NEXT: {}", fileChannel.position());

        return this;
    }

    // --- NEW findDataset using recursive in-memory traversal ---
    public HdfDataSet findDataset(String targetName, SeekableByteChannel fileChannel, HdfGroup hdfGroup) throws IOException {
        // Start recursive search on the in-memory B-tree
        Optional<HdfDataSet> dataset = findDatasetRecursive(
                hdfGroup.getBTree(), // Start at the root B-tree node of the group
                targetName,
                hdfGroup.getLocalHeapContents(), // Pass the heap for name lookups
                fileChannel // Pass the channel for reading object headers
        );

        return dataset.orElseThrow(() -> new IllegalArgumentException("No such dataset: " + targetName));
    }

    // --- Recursive helper for findDataset ---
    private Optional<HdfDataSet> findDatasetRecursive(HdfBTreeV1 currentNode,
                                                      String targetName,
                                                      HdfLocalHeapContents heapContents,
                                                      SeekableByteChannel fileChannel) throws IOException {

        if (currentNode == null) {
            return Optional.empty(); // Reached end of a branch unexpectedly
        }

        // Iterate through entries of the *current* in-memory node
        for (HdfBTreeEntry entry : currentNode.getEntries()) {
            if (entry == null) continue; // Skip if entry creation failed

            if (entry.isLeafEntry()) {
                // --- This entry points to a Symbol Table Node ---
                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
                if (snod != null) {
                    for (HdfSymbolTableEntry ste : snod.getSymbolTableEntries()) {
                        if (ste == null || ste.getLinkNameOffset() == null) continue;

                        HdfString linkName = heapContents.parseStringAtOffset(ste.getLinkNameOffset());
                        if (linkName != null && linkName.toString().equals(targetName)) {
                            // Found it by name! Now read its header.
                            long dataObjectHeaderAddress = ste.getObjectHeaderOffset().getInstance(Long.class);
                            long originalPos = fileChannel.position();
                            try {
                                fileChannel.position(dataObjectHeaderAddress);
                                HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize(), this);
                                DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class)
                                        .orElseThrow(() -> new IllegalStateException("No DatatypeMessage found for " + targetName));
                                log.debug("FOUND {}@{}\r\n{}", linkName, dataObjectHeaderAddress, header);
                                return Optional.of(new HdfDataSet(rootGroup, linkName.toString(), dataType.getHdfDatatype(), header));
                            } finally {
                                fileChannel.position(originalPos); // Restore position
                            }
                        }
                    }
                } else {
                    log.warn("Leaf BTree entry (Key: {}) points to a null SymbolTableNode.", entry.getKey());
                }
            } else if (entry.isInternalEntry()) {
                // --- This entry points to a child B-Tree node ---
                HdfBTreeV1 childBTree = entry.getChildBTree();
                if (childBTree != null) {
                    // Recurse down into the child B-tree
                    Optional<HdfDataSet> found = findDatasetRecursive(childBTree, targetName, heapContents, fileChannel);
                    if (found.isPresent()) {
                        return found; // Found it in the child branch
                    }
                } else {
                    log.warn("Internal BTree entry (Key: {}) points to a null child BTree.", entry.getKey());
                }
            }
        }

        return Optional.empty(); // Not found in this node or its descendants
    }

    // --- NEW getDatasets using recursive in-memory traversal ---
    public List<HdfDataSet> getDatasets(SeekableByteChannel fileChannel, HdfGroup hdfGroup) throws IOException {
        List<HdfDataSet> dataSets = new ArrayList<>();
        // Start recursive collection
        collectDatasetsRecursive(
                hdfGroup.getBTree(), // Start at the group's root B-tree node
                dataSets,            // List to populate
                hdfGroup.getLocalHeapContents(), // Heap for names
                fileChannel          // Channel for reading headers
        );
        return dataSets;
    }

    // --- Recursive helper for getDatasets ---
    private void collectDatasetsRecursive(HdfBTreeV1 currentNode,
                                          List<HdfDataSet> dataSets, // Accumulator list
                                          HdfLocalHeapContents heapContents,
                                          SeekableByteChannel fileChannel) throws IOException {

        if (currentNode == null) {
            return; // End of branch
        }

        for (HdfBTreeEntry entry : currentNode.getEntries()) {
            if (entry == null) continue;

            if (entry.isLeafEntry()) {
                // --- Process Leaf Entry (points to SNOD) ---
                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
                if (snod != null) {
                    for (HdfSymbolTableEntry ste : snod.getSymbolTableEntries()) {
                        if (ste == null || ste.getLinkNameOffset() == null || ste.getObjectHeaderOffset() == null) continue;

                        HdfString linkName = heapContents.parseStringAtOffset(ste.getLinkNameOffset());
                        if (linkName == null) continue; // Skip if name couldn't be parsed

                        long dataObjectHeaderAddress = ste.getObjectHeaderOffset().getInstance(Long.class);
                        long originalPos = fileChannel.position();
                        try {
                            fileChannel.position(dataObjectHeaderAddress);
                            HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize(), this);

                            // Check if it's a dataset by looking for DatatypeMessage
                            Optional<DatatypeMessage> dataTypeOpt = header.findMessageByType(DatatypeMessage.class);
                            if (dataTypeOpt.isPresent()) {
                                DatatypeMessage dataType = dataTypeOpt.get();
                                log.debug("Dataset {}@{}\r\n{}", linkName, dataObjectHeaderAddress, header);
                                dataSets.add(new HdfDataSet(rootGroup, linkName.toString(), dataType.getHdfDatatype(), header));
                            } else {
                                log.trace("Skipping non-dataset object '{}' at {}", linkName, dataObjectHeaderAddress);
                            }
                        } catch (Exception e) {
                            log.error("Failed to read or process object header for '{}' at {}. Skipping.", linkName, dataObjectHeaderAddress, e);
                        } finally {
                            fileChannel.position(originalPos); // Restore position
                        }
                    }
                } else {
                    log.warn("Leaf BTree entry (Key: {}) points to a null SymbolTableNode.", entry.getKey());
                }
            } else if (entry.isInternalEntry()) {
                // --- Recurse into Child B-Tree ---
                HdfBTreeV1 childBTree = entry.getChildBTree();
                if (childBTree != null) {
                    collectDatasetsRecursive(childBTree, dataSets, heapContents, fileChannel);
                } else {
                    log.warn("Internal BTree entry (Key: {}) points to a null child BTree.", entry.getKey());
                }
            }
        }
    }

    @Override
    public HdfGlobalHeap getGlobalHeap() {
        return globalHeap;
    }

    @Override
    public HdfFileAllocation getFileAllocation() {
        return fileAllocation;
    }
}