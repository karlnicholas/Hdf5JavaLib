package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.datatype.*;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.utils.*;
import lombok.Getter;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.dumpByteBuffer;

@Getter
public class HdfGroupManager {
    private HdfSymbolTableEntry rootGroupEntry;
    private HdfObjectHeaderPrefixV1 objectHeaderPrefix;
    private HdfLocalHeap localHeap;
    private HdfLocalHeapContents localHeapContents;
    private HdfBTreeV1 bTree;
    // initial setup without Dataset

    // Dataset description, plus what?
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;
    private HdfSymbolTableNode symbolTableNode;



    /**
     * Basic Allocation Type	Description
     * H5FD_MEM_SUPER	File space allocated for Superblock.
     * H5FD_MEM_BTREE	File space allocated for B-tree.
     * H5FD_MEM_DRAW	File space allocated for raw data.
     * H5FD_MEM_GHEAP	File space allocated for Global Heap.
     * H5FD_MEM_LHEAP	File space allocated for Local Heap.
     * H5FD_MEM_OHDR	File space allocated for Object Header.
     *
     */
    public void initializeNewHdfFile() {
        // Define a root group
        rootGroup(96);
        objectHeader();

        // Define the heap data size, why 88 I don't know.
        int dataSegmentSize = 88;
        // Initialize the heapData array
        byte[] heapData = new byte[dataSegmentSize];
        Arrays.fill(heapData, (byte) 0); // Set all bytes to 0

        localHeap(dataSegmentSize, 16, 712, heapData);

        // Define a B-Tree for group indexing
        addBTree();

    }

    private void localHeap(long dataSegmentSize, long freeListOffset, long dataSegmentAddress, byte[] data) {
        this.localHeap = new HdfLocalHeap("HEAP", 0,
                HdfFixedPoint.of(dataSegmentSize),
                HdfFixedPoint.of(freeListOffset),
                HdfFixedPoint.of(dataSegmentAddress));
        this.localHeapContents = new HdfLocalHeapContents(data);
    }

    /** Adds a group to the HDF5 file */
    private HdfGroupManager rootGroup(long objectHeaderAddress) {
        HdfFixedPoint objHeaderAddr = HdfFixedPoint.of(objectHeaderAddress);

        rootGroupEntry = new HdfSymbolTableEntry(HdfFixedPoint.of(0), objHeaderAddr, 1,
                HdfFixedPoint.of(136),
                HdfFixedPoint.of(680));
        return this;
    }

    /** Adds a group to the HDF5 file */
    private HdfGroupManager objectHeader() {
        objectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.of(136),
                        HdfFixedPoint.of(680))));

        return this;
    }

    /** Adds a dataset to the HDF5 file */
    public HdfGroupManager addDataset(List<HdfMessage> headerMessages) {
        int totalHeaderMessages = headerMessages.size();
        int objectReferenceCount = 1;
        int objectHeaderSize = 0;
        // 8, 1, 1064
        for( HdfMessage headerMessage: headerMessages ) {
            objectHeaderSize += headerMessage.getSizeMessageData();
        }
        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, totalHeaderMessages, objectReferenceCount, objectHeaderSize, headerMessages);

        return this;
    }

    /** Adds a B-Tree for Group Nodes */
    private void setBTreeGroupNode(long symbolTableAddress, String objectName) {
        List<HdfFixedPoint> childPointers = Collections.singletonList(HdfFixedPoint.of(symbolTableAddress));
        List<HdfFixedPoint> keys = Arrays.asList(
                HdfFixedPoint.of(0),
                HdfFixedPoint.of(8));

        List<BtreeV1GroupNode> groupNodes = Collections.singletonList(
                new BtreeV1GroupNode(new HdfString(objectName, false), HdfFixedPoint.of(symbolTableAddress)));

        bTree.setChildPointers(childPointers);
        bTree.setKeys(keys);
        bTree.setGroupNodes(groupNodes);

    }

    /** Adds a B-Tree for Group Nodes */
    private HdfGroupManager addBTree() {
        this.bTree = new HdfBTreeV1("TREE", 0, 0, 1,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8));
        return this;
    }

//    /** Adds a symbol table node */
//    private HdfGroupManager addSymbolTableNode(long objectHeaderAddress) {
//        return this;
//    }

    /** Writes the HDF5 file */
    public <T> void writeToFile(FileChannel fileChannel, HdfDataSet<T> hdfDataSet) throws IOException {
        // TODO: Implement actual serialization logic
//        System.out.println("Superblock: " + superblock);
        // Allocate a buffer of size 2208
        // Get the data address directly from the single dataObject
        HdfSuperblock superblock = hdfDataSet.getHdfFile().getSuperblock();
        Optional<HdfFixedPoint> optionalDataAddress = dataObjectHeaderPrefix.getDataAddress();

        // Extract the data start location dynamically
        long dataStart = optionalDataAddress
                .map(HdfFixedPoint::getBigIntegerValue)
                .map(BigInteger::longValue)
                .orElseThrow(() -> new IllegalStateException("No Data Layout Message found"));

        // Allocate the buffer dynamically up to the data start location
        ByteBuffer buffer = ByteBuffer.allocate((int) dataStart);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian

//        System.out.println(superblock);
        // Write the superblock at position 0
        buffer.position(0);
        superblock.writeToByteBuffer(buffer);

        System.out.println(rootGroupEntry);
        // Write the root group symbol table entry immediately after the superblock
        rootGroupEntry.writeToByteBuffer(buffer, superblock.getSizeOfOffsets());

        System.out.println(objectHeaderPrefix);
        // Write Object Header at position found in rootGroupEntry
        int objectHeaderAddress = rootGroupEntry.getObjectHeaderAddress().getBigIntegerValue().intValue();
        buffer.position(objectHeaderAddress);
        objectHeaderPrefix.writeToByteBuffer(buffer);

        long localHeapPosition = -1;
        long bTreePosition = -1;

        // Try getting the Local Heap Address from the Root Symbol Table Entry
        if (rootGroupEntry.getLocalHeapAddress() != null && !rootGroupEntry.getLocalHeapAddress().isUndefined()) {
            localHeapPosition = rootGroupEntry.getLocalHeapAddress().getBigIntegerValue().longValue();
        }

        // If not found or invalid, fallback to Object Header's SymbolTableMessage
        Optional<SymbolTableMessage> symbolTableMessageOpt = objectHeaderPrefix.findHdfSymbolTableMessage(SymbolTableMessage.class);
        if (symbolTableMessageOpt.isPresent()) {
            SymbolTableMessage symbolTableMessage = symbolTableMessageOpt.get();

            // Retrieve Local Heap Address if still not found
            if (localHeapPosition == -1 && symbolTableMessage.getLocalHeapAddress() != null && !symbolTableMessage.getLocalHeapAddress().isUndefined()) {
                localHeapPosition = symbolTableMessage.getLocalHeapAddress().getBigIntegerValue().longValue();
            }

            // Retrieve B-Tree Address
            if (symbolTableMessage.getBTreeAddress() != null && !symbolTableMessage.getBTreeAddress().isUndefined()) {
                bTreePosition = symbolTableMessage.getBTreeAddress().getBigIntegerValue().longValue();
            }
        }

        // Validate B-Tree Position and write it
        if (bTreePosition != -1) {
            System.out.println(bTree);
            buffer.position((int) bTreePosition); // Move to the correct position
            bTree.writeToByteBuffer(buffer);
        } else {
            throw new IllegalStateException("No valid B-Tree position found.");
        }

        // Validate Local Heap Position and write it
        if (localHeapPosition != -1) {
            buffer.position((int) localHeapPosition); // Move to the correct position
            localHeap.writeToByteBuffer(buffer);
            buffer.position(localHeap.getDataSegmentAddress().getBigIntegerValue().intValue());
            localHeapContents.writeToByteBuffer(buffer);
        } else {
            throw new IllegalStateException("No valid Local Heap position found.");
        }

        int objectDataHeaderAddress = symbolTableNode.getSymbolTableEntries().get(0).getObjectHeaderAddress().getBigIntegerValue().intValue();
        buffer.position(objectDataHeaderAddress);
        System.out.println(dataObjectHeaderPrefix);
        dataObjectHeaderPrefix.writeToByteBuffer(buffer);
        buffer.flip();
        dumpByteBuffer(buffer);
        fileChannel.write(buffer);
    }

    private long dataAddress() {
        return dataObjectHeaderPrefix
                .findHdfSymbolTableMessage(DataLayoutMessage.class)
                .orElseThrow()
                .getDataAddress()
                .getBigIntegerValue()
                .longValue();
    }

    public <T> HdfDataSet<T> createDataSet(HdfFile hdfFile, String datasetName, CompoundDataType compoundType, HdfFixedPoint[] hdfDimensions) {
        long objectHeaderAddress = 800;
        long symbolTableAddress = 1880;
        long datasetAddress = 2208;
        // Define a Symbol Table Node
        List<HdfSymbolTableEntry> entries = Collections.singletonList(
                new HdfSymbolTableEntry(HdfFixedPoint.of(8), HdfFixedPoint.of(objectHeaderAddress),
                        0,null, null));
        symbolTableNode = new HdfSymbolTableNode("SNOD", 1, 1, entries);

        setBTreeGroupNode(symbolTableAddress, datasetName);

        return new HdfDataSet<>(hdfFile, datasetName, compoundType, hdfDimensions, HdfFixedPoint.of(datasetAddress));
    }

    public <T> void closeDataSet(HdfDataSet<T> hdfDataSet, long messageCount) {
        // Initialize the localHeapContents heapData array
        System.arraycopy(hdfDataSet.getDatasetName().getBytes(StandardCharsets.US_ASCII), 0, getLocalHeapContents().getHeapData(), 8, hdfDataSet.getDatasetName().length());
        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(new ObjectHeaderContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
        headerMessages.add(new NilMessage());

        DatatypeMessage dataTypeMessage = new DatatypeMessage(1, 6, BitSet.valueOf(new byte[]{0b10001}), new HdfFixedPoint(false, new byte[]{(byte)56}, (short)4), hdfDataSet.getCompoundDataType());
//        dataTypeMessage.setDataType(compoundType);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        headerMessages.add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] hdfDimensionSizes = { HdfFixedPoint.of(messageCount)};
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1, HdfFixedPoint.of(2208), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined((short)8));
        headerMessages.add(dataLayoutMessage);

        // add ObjectModification Time message
        headerMessages.add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

        // Add DataspaceMessage (Handles dataset dimensionality)
//        HdfFixedPoint[] hdfDimensions = Arrays.stream(new long[]{1750}).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDataSet.getHdfDimensions(), hdfDataSet.getHdfDimensions(), true);
        headerMessages.add(dataSpaceMessage);

//        headerMessages.addAll(hdfDataSet.getAttributes());

        // new long[]{1750}, new long[]{98000}
        addDataset(headerMessages);

    }
}
