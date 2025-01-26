package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.utils.BtreeV1GroupNode;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.*;

public class HdfReader {
    // level 0
    private HdfSuperblock superblock;
    private HdfSymbolTableEntry rootGroupSymbolTableEntry;
    private HdfObjectHeaderV1 objectHeader;
    // level 1A
    private HdfBTreeV1 bTree;
    // level 1D
    private HdfLocalHeap localHeap;
    private HdfLocalHeapContents localHeapContents;
    // level 2A1
    private HdfDataObjectHeaderPrefixV1 dataObjectHeaderPrefix;
    // llevel 2AA
    private List<HdfMessage> dataObjectHeaderMessages;
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
        objectHeader = HdfObjectHeaderV1.readFromFileChannel(fileChannel, objectHeaderAddress, offsetSize, lengthSize);
        System.out.println(objectHeader);

        long bTreeAddress = objectHeader.getHdfSymbolTableMessage().getBTreeAddress().getBigIntegerValue().longValue();
        if ( superblock.getVersion() == 0 ) {
            System.out.print(bTreeAddress + " = ");
            bTree = HdfBTreeV1.readFromFileChannel(fileChannel, bTreeAddress, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());
            System.out.println(bTree);
        }

        // Parse the local heap using the file channel
        long localHeapAddress = objectHeader.getHdfSymbolTableMessage().getLocalHeapAddress().getBigIntegerValue().longValue();
        System.out.print(localHeapAddress + " = ");
        localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, localHeapAddress, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());
        System.out.println(localHeap);

        int dataSize = localHeap.getDataSegmentSize().getBigIntegerValue().intValue();
        long dataSegmentAddress = localHeap.getDataSegmentAddress().getBigIntegerValue().longValue();
        System.out.print(dataSegmentAddress + " = ");
        this.localHeapContents = HdfLocalHeapContents.readFromFileChannel(fileChannel, dataSize, dataSegmentAddress);
        System.out.println(localHeapContents);

        // Parse the Data Object Header Prefix next in line
        System.out.print(fileChannel.position() + " = ");
        dataObjectHeaderPrefix = HdfDataObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);
        System.out.println(dataObjectHeaderPrefix);

        // Parse header messages
        dataObjectHeaderMessages = new ArrayList<>();
        parseDataObjectHeaderMessages(fileChannel, (int)dataObjectHeaderPrefix.getObjectHeaderSize(), offsetSize, lengthSize, dataObjectHeaderMessages);
        for ( HdfMessage hdfMesage: dataObjectHeaderMessages) {
            if (hdfMesage instanceof ContinuationMessage) {
                parseContinuationMessage(fileChannel, (ContinuationMessage)hdfMesage, offsetSize, lengthSize, dataObjectHeaderMessages);
                break;
            }
        }
        dataObjectHeaderMessages.forEach(hm->System.out.println("\t" + hm));
        System.out.println("}");

        for (HdfMessage message : dataObjectHeaderMessages) {
            if ( message instanceof DataTypeMessage) {
                DataTypeMessage dataTypeMessage = (DataTypeMessage)  message;
                // Check if the datatype is Compound
                if (dataTypeMessage.getDataTypeClass() == 6) {
                    // Compound datatype
                    compoundDataType = new CompoundDataType(dataTypeMessage, dataTypeMessage.getData());
                } else {
                    // For other datatype classes, parsing logic will be added later
                    throw new UnsupportedOperationException("Datatype class " + dataTypeMessage.getDataTypeClass() + " not yet implemented.");
                }
            } else if ( message instanceof DataLayoutMessage) {
                DataLayoutMessage dataLayoutMessage = (DataLayoutMessage)  message;
                dataAddress = dataLayoutMessage.getDataAddress().getBigIntegerValue().longValue();
                dimensionSize = dataLayoutMessage.getDimensionSizes()[0].getBigIntegerValue().longValue();
            } else if ( message instanceof DataSpaceMessage) {
                DataSpaceMessage dataSpaceMessage = (DataSpaceMessage)  message;
                dimension = dataSpaceMessage.getDimensions()[0].getBigIntegerValue().longValue();
            }
        }
        BtreeV1GroupNode btreeV1GroupNode = parseBTreeAndLocalHeap(bTree, localHeapContents);
        System.out.println(btreeV1GroupNode);
        fileChannel.position(btreeV1GroupNode.getDataAddress().getBigIntegerValue().longValue());
        HdfSymbolTableNode hdfSymbolTableNode = HdfSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
        System.out.println(hdfSymbolTableNode);

        System.out.println("DataType{" + compoundDataType + "\r\n}");

        System.out.println("Parsing complete. NEXT: " + fileChannel.position());
    }

    public HdfSuperblock getSuperblock() {
        return superblock;
    }

    public HdfObjectHeaderV1 getObjectHeader() {
        return objectHeader;
    }

    public HdfLocalHeap getLocalHeap() {
        return localHeap;
    }

    public HdfLocalHeapContents getLocalHeapContents() {
        return localHeapContents;
    }

    public CompoundDataType getCompoundDataType() {
        return compoundDataType;
    }

    public long getDataAddress() {
        return dataAddress;
    }

    public long getDimension() {
        return dimension;
    }
}
