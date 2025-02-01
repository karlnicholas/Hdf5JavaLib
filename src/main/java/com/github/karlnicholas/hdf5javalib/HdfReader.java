package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.message.SymbolTableMessage;
import lombok.Getter;

import java.io.*;
import java.nio.channels.FileChannel;

@Getter
public class HdfReader {
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
    // parsed Datatype
    private CompoundDataType compoundDataType;
    private long dataAddress = 0;
    private long dimensionSize = 0;
    private long dimension = 0;

    public void readFile(FileChannel fileChannel) throws IOException {
        System.out.print(fileChannel.position() + " = ");
        // Parse the superblock at the beginning of the file
        superblock = HdfSuperblock.readFromFileChannel(fileChannel);
        System.out.println(superblock);
        // Parse root group symbol table entry from the current position
        System.out.print(fileChannel.position() + " = ");
        rootGroupSymbolTableEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, superblock.getSizeOfOffsets());
        System.out.println(rootGroupSymbolTableEntry);

        int offsetSize = superblock.getSizeOfOffsets();
        int lengthSize = superblock.getSizeOfLengths();

        // Get the object header address from the superblock
        long objectHeaderAddress =rootGroupSymbolTableEntry.getObjectHeaderAddress().getBigIntegerValue().longValue();
        // Parse the object header from the file using the superblock information
        System.out.print(objectHeaderAddress + " = ");
        fileChannel.position(objectHeaderAddress);
        objectHeaderPrefix = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);
        System.out.println(objectHeaderPrefix);

        // Parse the local heap using the file channel
        long localHeapAddress = objectHeaderPrefix.findHdfSymbolTableMessage(SymbolTableMessage.class)
                .orElseThrow().getLocalHeapAddress().getBigIntegerValue().longValue();
        System.out.print(localHeapAddress + " = ");
        localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, localHeapAddress, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());
        System.out.println(localHeap);

        int dataSize = localHeap.getDataSegmentSize().getBigIntegerValue().intValue();
        long dataSegmentAddress = localHeap.getDataSegmentAddress().getBigIntegerValue().longValue();
        fileChannel.position(dataSegmentAddress);
        System.out.print(dataSegmentAddress + " = ");
        this.localHeapContents = HdfLocalHeapContents.readFromFileChannel(fileChannel, dataSize);
        System.out.println(localHeapContents);


        if ( superblock.getVersion() == 0 ) {
            long bTreeAddress = objectHeaderPrefix.findHdfSymbolTableMessage(SymbolTableMessage.class)
                    .orElseThrow().getBTreeAddress().getBigIntegerValue().longValue();
            System.out.print(bTreeAddress + " = ");
            fileChannel.position(bTreeAddress);
            bTree = HdfBTreeV1.readFromFileChannel(fileChannel, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());
            bTree.parseBTreeAndLocalHeap(localHeapContents);
            System.out.println(bTree);
        }

        fileChannel.position(bTree.getGroupNodes().get(0).getDataAddress().getBigIntegerValue().longValue());
        HdfSymbolTableNode hdfSymbolTableNode = HdfSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
        System.out.println(hdfSymbolTableNode);

        // Parse the Data Object Header Prefix next in line
        fileChannel.position(hdfSymbolTableNode.getSymbolTableEntries().get(0).getObjectHeaderAddress().getBigIntegerValue().longValue());
        System.out.print(fileChannel.position() + " = ");
        dataObjectHeaderPrefix = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);
        System.out.println(dataObjectHeaderPrefix);

//        for (HdfMessage message : dataObjectHeaderPrefix.getDataObjectHeaderMessages()) {
//            if (message instanceof DataTypeMessage dataTypeMessage) {
//                // Check if the datatype is Compound
//                if (dataTypeMessage.getDataTypeClass() == 6) {
//                    compoundDataType = (CompoundDataType) dataTypeMessage.getHdfDataType();
//                } else {
//                    // For other datatype classes, parsing logic will be added later
//                    throw new UnsupportedOperationException("Datatype class " + dataTypeMessage.getDataTypeClass() + " not yet implemented.");
//                }
//            } else if (message instanceof DataLayoutMessage dataLayoutMessage) {
//                dataAddress = dataLayoutMessage.getDataAddress().getBigIntegerValue().longValue();
//                dimensionSize = dataLayoutMessage.getDimensionSizes()[0].getBigIntegerValue().longValue();
//            } else if (message instanceof DataSpaceMessage dataSpaceMessage) {
//                dimension = dataSpaceMessage.getDimensions()[0].getBigIntegerValue().longValue();
//            }
//        }
//
//        System.out.println("DataType{" + compoundDataType + "\r\n}");

        System.out.println("Parsing complete. NEXT: " + fileChannel.position());
    }
}
