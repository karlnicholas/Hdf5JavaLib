package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.*;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.utils.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.*;

public class HdfFileBuilder {
    private final HdfSuperblock superblock;
    private HdfSymbolTableEntry rootGroupEntry;
    private final List<HdfSymbolTableEntry> symbolTableEntries;
    private HdfObjectHeaderPrefixV1 objectHeaderPrefix;
    private final HdfLocalHeap localHeap;
    private final List<HdfBTreeV1> bTrees;
    private final List<HdfSymbolTableNode> symbolTableNodes;
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;
    private final Map<String, Long> objectNameToAddressMap;

    public HdfFileBuilder() {
        this.superblock = new HdfSuperblock(0, 0, 0, 0, 8, 8, 4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined(8),
                HdfFixedPoint.of(100320),
                HdfFixedPoint.undefined(8));

        this.symbolTableEntries = new ArrayList<>();
        this.localHeap = new HdfLocalHeap("HEAP", 0,
                HdfFixedPoint.of(88),
                HdfFixedPoint.of(16),
                HdfFixedPoint.of(712));
        this.bTrees = new ArrayList<>();
        this.symbolTableNodes = new ArrayList<>();
        this.objectNameToAddressMap = new HashMap<>();
    }

    /** Adds a group to the HDF5 file */
    public HdfFileBuilder rootGroup(long objectHeaderAddress) {
        HdfFixedPoint objHeaderAddr = HdfFixedPoint.of(objectHeaderAddress);

        rootGroupEntry = new HdfSymbolTableEntry(HdfFixedPoint.of(0), objHeaderAddr, 1,
                HdfFixedPoint.of(136),
                HdfFixedPoint.of(680));
//        symbolTableEntries.add(entry);
//
//        HdfObjectHeaderV1 header = new HdfObjectHeaderV1(1, 1, 1, 24,
//                Collections.singletonList(new SymbolTableMessage(
//                        HdfFixedPoint.of(136),
//                        HdfFixedPoint.of(680))));
//        objectHeaders.add(header);
//
//        objectNameToAddressMap.put(name, objectHeaderAddress);
        return this;
    }

    /** Adds a group to the HDF5 file */
    public HdfFileBuilder objectHeader() {
//        HdfFixedPoint objHeaderAddr = HdfFixedPoint.of(objectHeaderAddress);

//        HdfSymbolTableEntry entry = new HdfSymbolTableEntry(HdfFixedPoint.of(0), objHeaderAddr, 1,
//                HdfFixedPoint.of(136),
//                HdfFixedPoint.of(680));
//        symbolTableEntries.add(entry);

        objectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.of(136),
                        HdfFixedPoint.of(680))));

        return this;
    }

    /** Adds a dataset to the HDF5 file */
    public HdfFileBuilder addDataset(String name, long objectHeaderAddress, List<CompoundDataType.Member> members, long[] dimensions, long[] dimensionSizes) {
        HdfFixedPoint objHeaderAddr = HdfFixedPoint.of(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 dataObject = new HdfObjectHeaderPrefixV1(1, 8, 1, 1064, new ArrayList<>());

        // Continuation and Null Messages
        dataObject.getHeaderMessages().add(new ContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
        dataObject.getHeaderMessages().add(new NullMessage());

        // Define Compound DataType correctly
        CompoundDataType compoundType = new CompoundDataType(members.size(), 56, members);
        DataTypeMessage dataTypeMessage = new DataTypeMessage(1, 6, BitSet.valueOf(new long[]{0b10001}), HdfFixedPoint.of(56));
        dataTypeMessage.setDataType(compoundType);
        dataObject.getHeaderMessages().add(dataTypeMessage);

        // Add FillValue message
        dataObject.getHeaderMessages().add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] hdfDimensionSizes = Arrays.stream(dimensionSizes).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(1, 1, HdfFixedPoint.of(2208), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined(8));
        dataObject.getHeaderMessages().add(dataLayoutMessage);

        // add ObjectModification Time message
        dataObject.getHeaderMessages().add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

        // Add DataSpaceMessage (Handles dataset dimensionality)
        HdfFixedPoint[] hdfDimensions = Arrays.stream(dimensions).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataSpaceMessage dataSpaceMessage = new DataSpaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
        dataObject.getHeaderMessages().add(dataSpaceMessage);

        String attributeName = "GIT root revision";
        String attributeValue = "Revision: , URL: ";
        dataObject.getHeaderMessages().add(new AttributeMessage(1, name.length(), 8, 8, new HdfString(attributeName, false), attributeValue));

        // Store the dataset
        // TODO: Convert `data` into HDF5 binary format for actual writing
        // dataObject.setData(convertDataToHdf5Format(data));

        dataObjectHeaderPrefix = dataObject;
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
        // Allocate a buffer of size 2208
        // Get the data address directly from the single dataObject
        Optional<HdfFixedPoint> optionalDataAddress = dataObjectHeaderPrefix.getDataAddress();

        // Extract the data start location dynamically
        long dataStart = optionalDataAddress
                .map(HdfFixedPoint::getBigIntegerValue)
                .map(BigInteger::longValue)
                .orElseThrow(() -> new IllegalStateException("No Data Layout Message found"));

        // Allocate the buffer dynamically up to the data start location
        ByteBuffer buffer = ByteBuffer.allocate((int) dataStart);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian

        // Write the superblock at position 0
        buffer.position(0);
        superblock.writeToByteBuffer(buffer);

        // Write the root group symbol table entry immediately after the superblock
        rootGroupEntry.writeToByteBuffer(buffer, superblock.getSizeOfOffsets());

        // Step 3: Write Object Header at position found in rootGroupEntry
        long objectHeaderAddress = rootGroupEntry.getObjectHeaderAddress().getBigIntegerValue().longValue();
        objectHeaderPrefix.writeToByteBuffer(buffer, objectHeaderAddress, superblock.getSizeOfOffsets());

        System.out.println(superblock);
        symbolTableEntries.forEach(System.out::println);
        System.out.println(objectHeaderPrefix);
        bTrees.forEach(System.out::println);
        symbolTableNodes.forEach(System.out::println);
        System.out.println(dataObjectHeaderPrefix);

        dumpByteBuffer(buffer);
    }

    public static void dumpByteBuffer(ByteBuffer buffer) {
        int bytesPerLine = 16; // 16 bytes per row
        int limit = buffer.limit();
        buffer.rewind(); // Reset position to 0 before reading

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < limit; i += bytesPerLine) {
            // Print the address (memory offset in hex)
            sb.append(String.format("%08X:  ", i));

            // Print the first 8 bytes (hex values)
            for (int j = 0; j < 8; j++) {
                if (i + j < limit) {
                    sb.append(String.format("%02X ", buffer.get(i + j)));
                } else {
                    sb.append("   "); // Padding for incomplete lines
                }
            }

            sb.append(" "); // Space separator

            // Print the second 8 bytes (hex values)
            for (int j = 8; j < bytesPerLine; j++) {
                if (i + j < limit) {
                    sb.append(String.format("%02X ", buffer.get(i + j)));
                } else {
                    sb.append("   "); // Padding for incomplete lines
                }
            }

            // Newline for next row
            sb.append("\n");
        }

        System.out.print(sb.toString());
    }

}
