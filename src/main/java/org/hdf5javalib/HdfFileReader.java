package org.hdf5javalib;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.*;
import org.hdf5javalib.file.metadata.HdfSuperblock;

import java.io.IOException;
import java.nio.channels.FileChannel;
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

    private final HdfGlobalHeap globalHeap;
    private final HdfFileAllocation fileAllocation;

    public HdfFileReader() {
        this.fileAllocation = new HdfFileAllocation();
        this.globalHeap = new HdfGlobalHeap(this);
    }

    public void readFile(FileChannel fileChannel) throws IOException {
        // Parse the superblock at the beginning of the file
        superblock = HdfSuperblock.readFromFileChannel(fileChannel, this);
        log.debug("{}", superblock);

        short offsetSize = superblock.getOffsetSize();
        short lengthSize = superblock.getLengthSize();
//
//        rootSymbolTableEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
//
        // Get the object header address from the superblock
        // Parse the object header from the file using the superblock information
        long objectHeaderAddress = superblock.getRootGroupSymbolTableEntry().getObjectHeaderOffset().getInstance(Long.class);
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);

        // Parse the local heap using the file channel
        // Read data from file channel starting at the specified position
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
    }

//    public HdfDataSet findDataset(String targetName, FileChannel fileChannel, HdfGroup hdfGroup) throws IOException {
//        for (HdfBTreeEntry entry : hdfGroup.getBTree().getEntries()) {
//            HdfGroupSymbolTableNode symbolTableNode = entry.getSymbolTableNode();
//            for (HdfSymbolTableEntry ste : symbolTableNode.getSymbolTableEntries()) {
//                HdfString datasetName = hdfGroup.getLocalHeapContents().parseStringAtOffset(ste.getLinkNameOffset());
//                if (datasetName.toString().equals(targetName)) {
//                    long dataObjectHeaderAddress = ste.getObjectHeaderAddress().getInstance(Long.class);
//                    fileChannel.position(dataObjectHeaderAddress);
//                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());
//                    log.debug("FOUND {}@{}\r\n{}", datasetName, dataObjectHeaderAddress, header);
//                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow();
//                    return new HdfDataSet(rootGroup, datasetName.toString(), dataType.getHdfDatatype(), header);
//
//                }
//            }
//        }
//        throw new IllegalArgumentException("No such dataset: " + targetName);
//    }
//
//    public List<HdfDataSet> getDatasets(FileChannel fileChannel, HdfGroup hdfGroup) throws IOException {
//        List<HdfDataSet> dataSets = new ArrayList<>();
//        for (HdfBTreeEntry entry : hdfGroup.getBTree().getEntries()) {
//            HdfGroupSymbolTableNode symbolTableNode = entry.getSymbolTableNode();
//            for (HdfSymbolTableEntry ste : symbolTableNode.getSymbolTableEntries()) {
//                HdfString datasetName = hdfGroup.getLocalHeapContents().parseStringAtOffset(ste.getLinkNameOffset());
//                    long dataObjectHeaderAddress = ste.getObjectHeaderAddress().getInstance(Long.class);
//                    fileChannel.position(dataObjectHeaderAddress);
//                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());
//                    log.debug("FOUND {}@{}\r\n{}", datasetName, dataObjectHeaderAddress, header);
//                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow();
//                    dataSets.add( new HdfDataSet(rootGroup, datasetName.toString(), dataType.getHdfDatatype(), header));
//            }
//        }
//        return dataSets;
//    }
//
//    public HdfDataSet findDataset(String targetName, FileChannel fileChannel, HdfGroup hdfGroup) throws IOException {
//        for (HdfBTreeEntry entry : hdfGroup.getBTree().getEntries()) {
//            HdfGroupSymbolTableNode symbolTableNode = entry.getSymbolTableNode();
//            if (symbolTableNode == null) {
//                continue; // Skip internal node entries (shouldn’t happen with getLeafEntries, but safety first)
//            }
//            for (HdfSymbolTableEntry ste : symbolTableNode.getSymbolTableEntries()) {
//                if (ste == null || ste.getLinkNameOffset() == null) {
//                    continue; // Skip invalid entries
//                }
//                HdfString datasetName = hdfGroup.getLocalHeapContents().parseStringAtOffset(ste.getLinkNameOffset());
//                if (datasetName != null && datasetName.toString().equals(targetName)) {
//                    long dataObjectHeaderAddress = ste.getObjectHeaderAddress().getInstance(Long.class);
//                    long originalPos = fileChannel.position(); // Optional: save position
//                    fileChannel.position(dataObjectHeaderAddress);
//                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());
//                    log.debug("FOUND {}@{}\r\n{}", datasetName, dataObjectHeaderAddress, header);
//                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow(() -> new IllegalStateException("No DatatypeMessage found for " + datasetName));
//                    fileChannel.position(originalPos); // Optional: restore position
//                    return new HdfDataSet(rootGroup, datasetName.toString(), dataType.getHdfDatatype(), header);
//                }
//            }
//        }
//        throw new IllegalArgumentException("No such dataset: " + targetName);
//    }
//
//    public List<HdfDataSet> getDatasets(FileChannel fileChannel, HdfGroup hdfGroup) throws IOException {
//        List<HdfDataSet> dataSets = new ArrayList<>();
//        for (HdfBTreeEntry entry : hdfGroup.getBTree().getEntries()) {
//            HdfGroupSymbolTableNode symbolTableNode = entry.getSymbolTableNode();
//            if (symbolTableNode == null) {
//                continue; // Skip internal node entries
//            }
//            for (HdfSymbolTableEntry ste : symbolTableNode.getSymbolTableEntries()) {
//                if (ste == null || ste.getLinkNameOffset() == null) {
//                    continue; // Skip invalid entries
//                }
//                HdfString datasetName = hdfGroup.getLocalHeapContents().parseStringAtOffset(ste.getLinkNameOffset());
//                if (datasetName != null) {
//                    long dataObjectHeaderAddress = ste.getObjectHeaderAddress().getInstance(Long.class);
//                    long originalPos = fileChannel.position(); // Optional: save position
//                    fileChannel.position(dataObjectHeaderAddress);
//                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());
//                    log.debug("FOUND {}@{}\r\n{}", datasetName, dataObjectHeaderAddress, header);
//                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow(() -> new IllegalStateException("No DatatypeMessage found for " + datasetName));
//                    dataSets.add(new HdfDataSet(rootGroup, datasetName.toString(), dataType.getHdfDatatype(), header));
//                    fileChannel.position(originalPos); // Optional: restore position
//                }
//            }
//        }
//        return dataSets;
//    }


    // --- NEW findDataset using recursive in-memory traversal ---
    public HdfDataSet findDataset(String targetName, FileChannel fileChannel, HdfGroup hdfGroup) throws IOException {
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
                                                      FileChannel fileChannel) throws IOException {

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
                                HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());
                                DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class)
                                        .orElseThrow(() -> new IllegalStateException("Object '" + targetName + "' found but has no DatatypeMessage"));
                                log.debug("FOUND {}@{}\r\n{}", linkName, dataObjectHeaderAddress, header);
                                // Verify it's a dataset
                                // Assuming rootGroup as parent for now. Adjust if needed for sub-groups.
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
            // else: Entry is invalid/unrecognized? Log or ignore.
        }

        return Optional.empty(); // Not found in this node or its descendants
    }


    // --- NEW getDatasets using recursive in-memory traversal ---
    public List<HdfDataSet> getDatasets(FileChannel fileChannel, HdfGroup hdfGroup) throws IOException {
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
                                          FileChannel fileChannel) throws IOException {

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
                            HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());

                            // Check if it's a dataset by looking for DatatypeMessage
                            Optional<DatatypeMessage> dataTypeOpt = header.findMessageByType(DatatypeMessage.class);
                            if (dataTypeOpt.isPresent()) {
                                DatatypeMessage dataType = dataTypeOpt.get();
                                log.debug("Dataset {}@{}\r\n{}", linkName, dataObjectHeaderAddress, header);
                                dataSets.add(new HdfDataSet(rootGroup, linkName.toString(), dataType.getHdfDatatype(), header));
                            } else {
                                // It's some other linked object (like another group), ignore for getDatasets.
                                log.trace("Skipping non-dataset object '{}' at {}", linkName, dataObjectHeaderAddress);
                            }
                        } catch (Exception e) {
                            // Log error reading header but continue collecting others
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
            // else: Invalid entry type?
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
