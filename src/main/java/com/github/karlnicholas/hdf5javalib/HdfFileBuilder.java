package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.*;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.utils.*;

import java.lang.reflect.Array;
import java.time.Instant;
import java.util.*;

public class HdfFileBuilder {
    private final HdfSuperblock superblock;
    private final List<HdfSymbolTableEntry> symbolTableEntries;
    private final List<HdfObjectHeaderV1> objectHeaders;
    private final HdfLocalHeap localHeap;
    private final List<HdfBTreeV1> bTrees;
    private final List<HdfSymbolTableNode> symbolTableNodes;
    private final List<HdfDataObjectHeaderPrefixV1> dataObjects;
    private final Map<String, Long> objectNameToAddressMap;

    public HdfFileBuilder() {
        this.superblock = new HdfSuperblock(0, 0, 0, 0, 8, 8, 4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined(8),
                HdfFixedPoint.of(100320),
                HdfFixedPoint.undefined(8));

        this.symbolTableEntries = new ArrayList<>();
        this.objectHeaders = new ArrayList<>();
        this.localHeap = new HdfLocalHeap("HEAP", 0,
                HdfFixedPoint.of(88),
                HdfFixedPoint.of(16),
                HdfFixedPoint.of(712));
        this.bTrees = new ArrayList<>();
        this.symbolTableNodes = new ArrayList<>();
        this.dataObjects = new ArrayList<>();
        this.objectNameToAddressMap = new HashMap<>();
    }

    /** Adds a group to the HDF5 file */
    public HdfFileBuilder addGroup(String name, long objectHeaderAddress) {
        HdfFixedPoint objHeaderAddr = HdfFixedPoint.of(objectHeaderAddress);

        HdfSymbolTableEntry entry = new HdfSymbolTableEntry(HdfFixedPoint.of(0), objHeaderAddr, 1,
                HdfFixedPoint.of(136),
                HdfFixedPoint.of(680));
        symbolTableEntries.add(entry);

        HdfObjectHeaderV1 header = new HdfObjectHeaderV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.of(136),
                        HdfFixedPoint.of(680))));
        objectHeaders.add(header);

        objectNameToAddressMap.put(name, objectHeaderAddress);
        return this;
    }

    /** Adds a dataset to the HDF5 file */
    public HdfFileBuilder addDataset(String name, long objectHeaderAddress, List<CompoundDataType.Member> members, long[] dimensions, long[] dimensionSizes) {
        HdfFixedPoint objHeaderAddr = HdfFixedPoint.of(objectHeaderAddress);
        HdfDataObjectHeaderPrefixV1 dataObject = new HdfDataObjectHeaderPrefixV1(1, 8, 1, 1064, new ArrayList<>());

        // Continuation and Null Messages
        dataObject.getDataObjectHeaderMessages().add(new ContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
        dataObject.getDataObjectHeaderMessages().add(new NullMessage());

        // Define Compound DataType correctly
        CompoundDataType compoundType = new CompoundDataType(members.size(), 56, members);
        DataTypeMessage dataTypeMessage = new DataTypeMessage(1, 6, BitSet.valueOf(new long[]{0b10001}), HdfFixedPoint.of(56));
        dataTypeMessage.setDataType(compoundType);
        dataObject.getDataObjectHeaderMessages().add(dataTypeMessage);

        // Add FillValue message
        dataObject.getDataObjectHeaderMessages().add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] hdfDimensionSizes = Arrays.stream(dimensionSizes).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(1, 1, HdfFixedPoint.of(2208), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined(8));
        dataObject.getDataObjectHeaderMessages().add(dataLayoutMessage);

        // add ObjectModification Time message
        dataObject.getDataObjectHeaderMessages().add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

        // Add DataSpaceMessage (Handles dataset dimensionality)
        HdfFixedPoint[] hdfDimensions = Arrays.stream(dimensions).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataSpaceMessage dataSpaceMessage = new DataSpaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
        dataObject.getDataObjectHeaderMessages().add(dataSpaceMessage);

        String attributeName = "GIT root revision";
        String attributeValue = "Revision: , URL: ";
        dataObject.getDataObjectHeaderMessages().add(new AttributeMessage(1, name.length(), 8, 8, new HdfString(attributeName, false), attributeValue));

        // Store the dataset
        // TODO: Convert `data` into HDF5 binary format for actual writing
        // dataObject.setData(convertDataToHdf5Format(data));

        dataObjects.add(dataObject);
        objectNameToAddressMap.put(name, objectHeaderAddress);
        return this;
    }

    /** Adds a B-Tree for Group Nodes */
    public HdfFileBuilder addBTree(long address, String objectName) {
        List<HdfFixedPoint> childPointers = Collections.singletonList(HdfFixedPoint.of(address));
        List<HdfFixedPoint> keys = Arrays.asList(
                HdfFixedPoint.of(0),
                HdfFixedPoint.of(8));

        List<BtreeV1GroupNode> groupNodes = Collections.singletonList(
                new BtreeV1GroupNode(new HdfString(objectName, false), HdfFixedPoint.of(address)));

        HdfBTreeV1 bTree = new HdfBTreeV1("TREE", 0, 0, 1,
                HdfFixedPoint.undefined(8),
                HdfFixedPoint.undefined(8),
                childPointers, keys, groupNodes);
        bTrees.add(bTree);
        return this;
    }

    /** Adds a symbol table node */
    public HdfFileBuilder addSymbolTableNode(long objectHeaderAddress) {
        List<HdfSymbolTableEntry> entries = Collections.singletonList(
                new HdfSymbolTableEntry(HdfFixedPoint.of(8), HdfFixedPoint.of(objectHeaderAddress),
                        0,null, null));
        HdfSymbolTableNode node = new HdfSymbolTableNode("SNOD", 1, 1, entries);
        symbolTableNodes.add(node);
        return this;
    }

    /** Writes the HDF5 file */
    public void writeToFile(String filePath) {
        // TODO: Implement actual serialization logic
        System.out.println("Writing HDF5 file to " + filePath);
        System.out.println("Superblock: " + superblock);
        symbolTableEntries.forEach(System.out::println);
        objectHeaders.forEach(System.out::println);
        bTrees.forEach(System.out::println);
        symbolTableNodes.forEach(System.out::println);
        dataObjects.forEach(System.out::println);
    }
}
