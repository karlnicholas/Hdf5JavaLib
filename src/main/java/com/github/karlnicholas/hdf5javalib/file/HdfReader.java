package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.data.HdfString;
import com.github.karlnicholas.hdf5javalib.datatype.HdfDatatype;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.*;
import lombok.Getter;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

@Getter
public class HdfReader {
    // level 0
    private HdfSuperblock superblock;
    // level 1
    private HdfGroup rootGroup;
    // level 2A1
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;
    // parsed Datatype
    private HdfDatatype dataType;
    private long dataAddress = 0;
    private long dimensionSize = 0;
    private long dimension = 0;

    public void readFile(FileChannel fileChannel) throws IOException {
        // Parse the superblock at the beginning of the file
        superblock = HdfSuperblock.readFromFileChannel(fileChannel);
        System.out.println(superblock);

        short offsetSize = superblock.getSizeOfOffsets();
        short lengthSize = superblock.getSizeOfLengths();

        HdfSymbolTableEntry fileOffsets = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);

        // Get the object header address from the superblock
        // Parse the object header from the file using the superblock information
        long objectHeaderAddress =fileOffsets.getObjectHeaderAddress().getBigIntegerValue().longValue();
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);

        // Parse the local heap using the file channel
        // Read data from file channel starting at the specified position
        long localHeapAddress = objectHeader.findHdfSymbolTableMessage(SymbolTableMessage.class)
                .orElseThrow().getLocalHeapAddress().getBigIntegerValue().longValue();
        fileChannel.position(localHeapAddress);
        HdfLocalHeap localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());

        int dataSize = localHeap.getDataSegmentSize().getBigIntegerValue().intValue();
        long dataSegmentAddress = localHeap.getDataSegmentAddress().getBigIntegerValue().longValue();
        fileChannel.position(dataSegmentAddress);
        HdfLocalHeapContents localHeapContents = HdfLocalHeapContents.readFromFileChannel(fileChannel, dataSize);

        long bTreeAddress = objectHeader.findHdfSymbolTableMessage(SymbolTableMessage.class)
                .orElseThrow().getBTreeAddress().getBigIntegerValue().longValue();
        fileChannel.position(bTreeAddress);
        HdfBTreeV1 bTree = HdfBTreeV1.readFromFileChannel(fileChannel, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());

        // Parse root group symbol table entry from the current position
        HdfSymbolTableEntry rootGroupSymbolTableEntry = new HdfSymbolTableEntry(
                HdfFixedPoint.of(0),
                HdfFixedPoint.of(objectHeaderAddress),
                1,
                HdfFixedPoint.of(bTreeAddress),
                HdfFixedPoint.of(localHeapAddress)
        );
        // get datasets?
        if ( bTree.getEntriesUsed() != 1) {
            throw new UnsupportedEncodingException("Only one btree entry is supported");
        }
        BTreeEntry bTreeEntry = bTree.getEntries().get(0);
        long snodAddress = bTreeEntry.getChildPointer().getBigIntegerValue().longValue();
        fileChannel.position(snodAddress);
        HdfGroupSymbolTableNode symbolTableNode = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
        int entriesToRead = symbolTableNode.getNumberOfSymbols();
        for(int i=0; i <entriesToRead; ++i) {
            HdfSymbolTableEntry symbolTableEntry  = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
            symbolTableNode.getSymbolTableEntries().add(symbolTableEntry);
        }
        rootGroup = new HdfGroup(
                "",
                rootGroupSymbolTableEntry,
                objectHeader,
                bTree,
                localHeap,
                localHeapContents,
                symbolTableNode
        );


//            System.out.println(hdfGroupSymbolTableNode);

        System.out.println(rootGroup);

        for( int i=0; i < symbolTableNode.getNumberOfSymbols(); ++i ) {
            HdfSymbolTableEntry ste = symbolTableNode.getSymbolTableEntries().get(i);
            HdfString datasetName = localHeapContents.parseStringAtOffset(ste.getLinkNameOffset());
            // Parse the Data Object Header Prefix next in line
//        fileChannel.position(fileOffsets.getObjectHeaderAddress().getBigIntegerValue().longValue());
            long dataLObjectHeaderAddress = ste.getObjectHeaderAddress().getBigIntegerValue().longValue();
            fileChannel.position(dataLObjectHeaderAddress);
            dataObjectHeaderPrefix = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);
            System.out.println(datasetName + "@" + dataLObjectHeaderAddress + " = " + dataObjectHeaderPrefix);

            for (HdfMessage message : dataObjectHeaderPrefix.getHeaderMessages()) {
                if (message instanceof DatatypeMessage dataTypeMessage) {
//                // Check if the datatype is Compound
//                if (dataTypeMessage.getDataTypeClass() == 6) {
//                    dataType = dataTypeMessage.getHdfDatatype();
//                } else {
//                    // For other datatype classes, parsing logic will be added later
//                    throw new UnsupportedOperationException("Datatype class " + dataTypeMessage.getDataTypeClass() + " not yet implemented.");
//                }
                    dataType = dataTypeMessage.getHdfDatatype();
                } else if (message instanceof DataLayoutMessage dataLayoutMessage) {
                    dataAddress = dataLayoutMessage.getDataAddress().getBigIntegerValue().longValue();
                    dimensionSize = dataLayoutMessage.getDimensionSizes()[0].getBigIntegerValue().longValue();
                } else if (message instanceof DataspaceMessage dataSpaceMessage) {
                    dimension = dataSpaceMessage.getDimensions()[0].getBigIntegerValue().longValue();
                }
            }

        }


//        System.out.println("DataType{" + compoundDataType + "\r\n}");

        System.out.println("Parsing complete. NEXT: " + fileChannel.position());
    }
}
