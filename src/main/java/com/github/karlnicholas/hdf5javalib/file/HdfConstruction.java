package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfBTreeV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfLocalHeap;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfLocalHeapContents;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.BtreeV1GroupNode;

import java.math.BigInteger;
import java.util.*;

public class HdfConstruction {
    // level 0
    private HdfSuperblock superblock;
    private HdfSymbolTableEntry rootGroupSymbolTableEntry;
    private HdfObjectHeaderPrefixV1 objectHeaderPrefix;
    // level 1A
    private HdfBTreeV1 bTree;
    // level 1D
    private HdfLocalHeap localHeap;
    private HdfLocalHeapContents localHeapContents;
    // level 2A1
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;

    public void buildHfd() {
        buildSuperblock();
        buildRootSymbolTableEntry();
        buildObjectHeader();
        buildBTree();
        buildLocalHeap();
        buildLocalHeapContents();
        buildDataObjectHeaderPrefix();

        System.out.println("Building HFD");
        System.out.println("Superblock: " + superblock);
        System.out.println("Root symbol table entry: " + rootGroupSymbolTableEntry);
        System.out.println("Object header: " + objectHeaderPrefix);
        System.out.println("BTree: " + bTree);
        System.out.println("Local heap: " + localHeap);
        System.out.println("Local heap contents: " + localHeapContents);
        System.out.println("DataObject header: " + dataObjectHeaderPrefix);

    }

    private void buildSuperblock() {
        // Create "undefined" HdfFixedPoint instances
        HdfFixedPoint undefinedAddress = HdfFixedPoint.undefined((short) 8); // Size of offsets = 8

        // Create specific HdfFixedPoint instances for defined values
        HdfFixedPoint baseAddress = new HdfFixedPoint(BigInteger.ZERO, true); // Base address = 0, little-endian
        HdfFixedPoint endOfFileAddress = new HdfFixedPoint(BigInteger.valueOf(100320), true); // EOF address = 100320, little-endian

        // Construct the HdfSuperblock instance
        superblock = new HdfSuperblock(
                0,                          // version
                0,                          // freeSpaceVersion
                0,                          // rootGroupVersion
                0,                          // sharedHeaderVersion
                (short)8,                          // sizeOfOffsets
                (short)8,                          // sizeOfLengths
                4,                          // groupLeafNodeK
                16,                         // groupInternalNodeK
                baseAddress,                // baseAddress
                undefinedAddress,           // freeSpaceAddress
                endOfFileAddress,           // endOfFileAddress
                undefinedAddress            // driverInformationAddress
        );
    }

    private void buildRootSymbolTableEntry() {
        // Create specific HdfFixedPoint instances for the required addresses
        HdfFixedPoint linkNameOffset = new HdfFixedPoint(BigInteger.ZERO, true); // Link name offset = 0
        HdfFixedPoint objectHeaderAddress = new HdfFixedPoint(BigInteger.valueOf(96), true); // Object header address = 96
        HdfFixedPoint bTreeAddress = new HdfFixedPoint(BigInteger.valueOf(136), true); // B-tree address = 136
        HdfFixedPoint localHeapAddress = new HdfFixedPoint(BigInteger.valueOf(680), true); // Local heap address = 680

        // Construct the HdfSymbolTableEntry instance
        rootGroupSymbolTableEntry = new HdfSymbolTableEntry(
                linkNameOffset,
                objectHeaderAddress,
                1,                // Cache type = 1
                bTreeAddress,
                localHeapAddress
        );
    }

    private void buildObjectHeader() {
        // Create HdfFixedPoint instances for the SymbolTableMessage
        HdfFixedPoint bTreeAddress = new HdfFixedPoint(BigInteger.valueOf(136), true); // B-tree address = 136
        HdfFixedPoint localHeapAddress = new HdfFixedPoint(BigInteger.valueOf(680), true); // Local heap address = 680

        // Create a SymbolTableMessage directly
        SymbolTableMessage symbolTableMessage = new SymbolTableMessage(bTreeAddress, localHeapAddress);

        // Create the HdfObjectHeaderV1 instance
        objectHeaderPrefix = new HdfObjectHeaderPrefixV1(
                1, // Version
                1, // Total header messages
                1, // Object reference count
                24, // Object header size
                Collections.singletonList(symbolTableMessage) // List of header messages
        );
    }

    private void buildBTree() {
        // Define the fixed values for the HdfBTreeV1 instance
        String signature = "TREE";
        int nodeType = 0;
        int nodeLevel = 0;
        int entriesUsed = 1;

        // Create undefined HdfFixedPoint values for sibling addresses
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.undefined((short)8);
        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.undefined((short)8);

        // Create the childPointers and keys
        List<HdfFixedPoint> childPointers = Collections.singletonList(new HdfFixedPoint(BigInteger.valueOf(1880), true));
        List<HdfFixedPoint> keys = new ArrayList<>();
        keys.add(new HdfFixedPoint(BigInteger.valueOf(0), true)); // First key
        keys.add(new HdfFixedPoint(BigInteger.valueOf(8), true)); // Final key

        // Create a list of BtreeV1GroupNode objects
        List<BtreeV1GroupNode> groupNodes = new ArrayList<>();
        groupNodes.add(new BtreeV1GroupNode(new HdfString("Demand", false), new HdfFixedPoint(BigInteger.valueOf(1880), true)));

        // Construct the HdfBTreeV1 instance with group nodes
        bTree = new HdfBTreeV1(
                signature,
                nodeType,
                nodeLevel,
                entriesUsed,
                leftSiblingAddress,
                rightSiblingAddress,
                childPointers,
                keys,
                groupNodes
        );
    }


    private void buildLocalHeap() {
        // Define the fixed values for the HdfLocalHeap instance
        String signature = "HEAP";
        int version = 0;

        // Create the HdfFixedPoint values
        HdfFixedPoint dataSegmentSize = new HdfFixedPoint(BigInteger.valueOf(88), true); // Little-endian
        HdfFixedPoint freeListOffset = new HdfFixedPoint(BigInteger.valueOf(16), true); // Little-endian
        HdfFixedPoint dataSegmentAddress = new HdfFixedPoint(BigInteger.valueOf(712), true); // Little-endian

        // Construct the HdfLocalHeap instance
        localHeap = new HdfLocalHeap(
                signature,
                version,
                dataSegmentSize,
                freeListOffset,
                dataSegmentAddress
        );
    }

    private void buildLocalHeapContents() {
        // Define the heap data size
        int heapDataSize = 88;

        // Initialize the heapData array
        byte[] heapData = new byte[heapDataSize];
        Arrays.fill(heapData, (byte) 0); // Set all bytes to 0

        // Set bytes 8-16 to the required values
        heapData[8] = 68;  // 'D'
        heapData[9] = 101; // 'e'
        heapData[10] = 109; // 'm'
        heapData[11] = 97; // 'a'
        heapData[12] = 110; // 'n'
        heapData[13] = 100; // 'd'
        heapData[14] = 0;  // null terminator
        heapData[15] = 0;  // additional null byte

        // Create the HdfLocalHeapContents instance using the updated constructor
        localHeapContents = new HdfLocalHeapContents(heapData);
    }

    public void buildDataObjectHeaderPrefix() {
        // ObjectHeaderContinuationMessage
        ObjectHeaderContinuationMessage objectHeaderContinuationMessage = new ObjectHeaderContinuationMessage(
                new HdfFixedPoint(BigInteger.valueOf(100208), false),
                new HdfFixedPoint(BigInteger.valueOf(112), false));

        // NilMessage
        NilMessage nilMessage = new NilMessage();

        // DatatypeMessage with CompoundDataType
        List<CompoundDataType.Member> members = List.of(
                new CompoundDataType.Member("Id", 0, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)8, false, false, false, false, (short)0, (short)64, (short) ("shipmentId".length()+9), new BitSet())),
                new CompoundDataType.Member("origCountry", 8, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte)1, (short)2, 0, "Null Terminate", 0, "ASCII", (short) ("origCountry".length()+9))),
                new CompoundDataType.Member("origSlic", 10, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte)1, (short)5, 0, "Null Terminate", 0, "ASCII", (short) ("origSlic".length()+9))),
                new CompoundDataType.Member("origSort", 15, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)1, false, false, false, false, (short)0, (short)8, (short) ("origSort".length()+9), new BitSet())),
                new CompoundDataType.Member("destCountry", 16, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte)1, (short)2, 0, "Null Terminate", 0, "ASCII", (short) ("destCountry".length()+9))),
                new CompoundDataType.Member("destSlic", 18, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte)1, (short)5, 0, "Null Terminate", 0, "ASCII", (short) ("destSlic".length()+9))),
                new CompoundDataType.Member("destIbi", 23, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)1, false, false, false, false, (short)0, (short)8, (short) ("destIbi".length()+9), new BitSet())),
                new CompoundDataType.Member("destPostalCode", 40, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte)1, (short)9, 0, "Null Terminate", 0, "ASCII", (short) ("destPostalCode".length()+9))),
                new CompoundDataType.Member("shipper", 24, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte)1, (short)10, 0, "Null Terminate", 0, "ASCII", (short) ("shipper".length()+9))),
                new CompoundDataType.Member("service", 49, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)1, false, false, false, false, (short)0, (short)8, (short) ("service".length()+9), new BitSet())),
                new CompoundDataType.Member("packageType", 50, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)1, false, false, false, false, (short)0, (short)8, (short) ("packageType".length()+9), new BitSet())),
                new CompoundDataType.Member("accessorials", 51, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)1, false, false, false, false, (short)0, (short)8, (short) ("accessorials".length()+9), new BitSet())),
                new CompoundDataType.Member("pieces", 52, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)2, false, false, false, false, (short)0, (short)16, (short) ("pieces".length()+9), new BitSet())),
                new CompoundDataType.Member("weight", 34, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)2, false, false, false, false, (short)0, (short)16, (short) ("weight".length()+9), new BitSet())),
                new CompoundDataType.Member("cube", 36, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)4, false, false, false, false, (short)0, (short)32, (short) ("cube".length()+9), new BitSet())),
                new CompoundDataType.Member("committedTnt", 54, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)1, false, false, false, false, (short)0, (short)8, (short) ("committedTnt".length()+9), new BitSet())),
                new CompoundDataType.Member("committedDate", 55, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte)1, (short)1, false, false, false, false, (short)0, (short)8, (short) ("committedDate".length()+9), new BitSet()))
        );

        CompoundDataType compoundDataType = new CompoundDataType(17, 56, members);
        DatatypeMessage dataTypeMessage = new DatatypeMessage(1, 6, BitSet.valueOf(new long[]{17}), new HdfFixedPoint(BigInteger.valueOf(56), false), compoundDataType );
//        dataTypeMessage.setDataType(compoundDataType);

        // FillValueMessage
        FillValueMessage fillValueMessage = new FillValueMessage(2, 2, 2, 1, null, null);

        // DataLayoutMessage
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(
                3, 1,
                new HdfFixedPoint(BigInteger.valueOf(2208), false),
                new HdfFixedPoint[]{new HdfFixedPoint(BigInteger.valueOf(98000), false)},
                0,
                null,
                null
        );

        // ObjectModificationTimeMessage
        ObjectModificationTimeMessage objectModificationTimeMessage = new ObjectModificationTimeMessage(1, 1241645056);
        new HdfFixedPoint(BigInteger.valueOf(1750), true);
        // DataspaceMessage
        DataspaceMessage dataSpaceMessage = new DataspaceMessage(
                1,
                1,
                1,
                new HdfFixedPoint[]{new HdfFixedPoint(BigInteger.valueOf(1750), false)},
                new HdfFixedPoint[]{new HdfFixedPoint(BigInteger.valueOf(1750), false)},
                true
        );

        String attributeName = "GIT root revision";
        String attributeValue = "Revision: , URL: ";
        DatatypeMessage dt = new DatatypeMessage(1, 3, BitSet.valueOf(new byte[0]), HdfFixedPoint.of(attributeName.length()+1), new HdfString(attributeName, false));
        DataspaceMessage ds = new DataspaceMessage(1, 1, 1, new HdfFixedPoint[] {HdfFixedPoint.of(1)}, null, false);
        HdfFixedPoint.of(attributeValue.length()+1);
        AttributeMessage attributeMessage = new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), new HdfString(attributeValue, false));
        // AttributeMessage

        // Combine all messages
        List<HdfMessage> dataObjectHeaderMessages = Arrays.asList(
                objectHeaderContinuationMessage,
                nilMessage,
                dataTypeMessage,
                fillValueMessage,
                dataLayoutMessage,
                objectModificationTimeMessage,
                dataSpaceMessage,
                attributeMessage
        );

        // Construct and return the instance
        dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, 8, 1, 1064, dataObjectHeaderMessages);
    }

}