package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.datatype.*;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.*;
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
    // initial setup without Dataset
    private final HdfFile hdfFile;

    // Dataset description, plus what?
//    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;
//    private List<HdfGroup> groups;
    private HdfSymbolTableNode  symbolTableNode;
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;

    public HdfGroupManager(HdfFile hdfFile) {
        this.hdfFile = hdfFile;
    }

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
    private void setBTreeGroupNode(long symbolTableAddress, String objectName, long objectHeaderAddress) {
        List<HdfFixedPoint> childPointers = Collections.singletonList(HdfFixedPoint.of(symbolTableAddress));
        List<HdfFixedPoint> keys = Arrays.asList(
                HdfFixedPoint.of(0),
                HdfFixedPoint.of(8));

        List<BtreeV1GroupNode> groupNodes = Collections.singletonList(
                new BtreeV1GroupNode(new HdfString(objectName, false), HdfFixedPoint.of(symbolTableAddress)));

        // Define a Symbol Table Node
        List<HdfSymbolTableEntry> entries = Collections.singletonList(
                new HdfSymbolTableEntry(HdfFixedPoint.of(8), HdfFixedPoint.of(objectHeaderAddress),
                        0,null, null));
        HdfSymbolTableNode symbolTableNode = new HdfSymbolTableNode("SNOD", 1, 1, entries);

        hdfFile.getBTree().setChildPointers(childPointers);
        hdfFile.getBTree().setKeys(keys);
        hdfFile.getBTree().setGroupNodes(groupNodes);

    }
//    /** Adds a symbol table node */
//    private HdfGroupManager addSymbolTableNode(long objectHeaderAddress) {
//        return this;
//    }

    /** Writes the HDF5 file */
    public <T> void writeToFile(FileChannel fileChannel, HdfDataSet<T> hdfDataSet) throws IOException {

//        int objectDataHeaderAddress = symbolTableNode.getSymbolTableEntries().get(0).getObjectHeaderAddress().getBigIntegerValue().intValue();
//        buffer.position(objectDataHeaderAddress);
//        System.out.println(dataObjectHeaderPrefix);
//        dataObjectHeaderPrefix.writeToByteBuffer(buffer);
//        buffer.flip();
//        dumpByteBuffer(buffer);
//        fileChannel.write(buffer);
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

        setBTreeGroupNode(symbolTableAddress, datasetName, objectHeaderAddress);

        return new HdfDataSet<>(hdfFile, datasetName, compoundType, hdfDimensions, HdfFixedPoint.of(datasetAddress));
    }

    public <T> void closeDataSet(HdfDataSet<T> hdfDataSet, long messageCount) {
        // Initialize the localHeapContents heapData array
        System.arraycopy(hdfDataSet.getDatasetName().getBytes(StandardCharsets.US_ASCII), 0, hdfFile.getLocalHeapContents().getHeapData(), 8, hdfDataSet.getDatasetName().length());
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
