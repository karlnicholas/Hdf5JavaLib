package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.data.HdfString;
import com.github.karlnicholas.hdf5javalib.message.datatype.HdfDatatype;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.DataLayoutMessage;
import com.github.karlnicholas.hdf5javalib.message.DataspaceMessage;
import com.github.karlnicholas.hdf5javalib.message.DatatypeMessage;
import com.github.karlnicholas.hdf5javalib.message.HdfMessage;
import lombok.Getter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;

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
//
//        rootSymbolTableEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
//
        // Get the object header address from the superblock
        // Parse the object header from the file using the superblock information
        long objectHeaderAddress = superblock.getRootGroupSymbolTableEntry().getObjectHeaderAddress().toBigInteger().longValue();
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);

        // Parse the local heap using the file channel
        // Read data from file channel starting at the specified position
        long localHeapAddress = superblock.getRootGroupSymbolTableEntry().getNameHeapAddress().toBigInteger().longValue();
        fileChannel.position(localHeapAddress);
        HdfLocalHeap localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());

        int dataSize = localHeap.getDataSegmentSize().toBigInteger().intValue();
        long dataSegmentAddress = localHeap.getDataSegmentAddress().toBigInteger().longValue();
        fileChannel.position(dataSegmentAddress);
        HdfLocalHeapContents localHeapContents = HdfLocalHeapContents.readFromFileChannel(fileChannel, dataSize);

        long bTreeAddress = superblock.getRootGroupSymbolTableEntry().getBTreeAddress().toBigInteger().longValue();
        fileChannel.position(bTreeAddress);
        HdfBTreeV1 bTree = HdfBTreeV1.readFromFileChannel(fileChannel, superblock.getSizeOfOffsets(), superblock.getSizeOfLengths());

        // get datasets?
        if ( bTree.getEntriesUsed() != 1) {
            throw new UnsupportedEncodingException("Only one btree entry is currently supported");
        }
        BTreeEntry bTreeEntry = bTree.getEntries().get(0);
        long snodAddress = bTreeEntry.getChildPointer().toBigInteger().longValue();
        fileChannel.position(snodAddress);
        HdfGroupSymbolTableNode symbolTableNode = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
        int entriesToRead = symbolTableNode.getNumberOfSymbols();
        for(int i=0; i <entriesToRead; ++i) {
            HdfSymbolTableEntry symbolTableEntry  = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
            symbolTableNode.getSymbolTableEntries().add(symbolTableEntry);
        }
        rootGroup = new HdfGroup(
                null,
                "",
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
//        fileChannel.position(fileOffsets.getObjectHeaderAddress().toBigInteger().longValue());
            long dataLObjectHeaderAddress = ste.getObjectHeaderAddress().toBigInteger().longValue();
            fileChannel.position(dataLObjectHeaderAddress);
            dataObjectHeaderPrefix = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);
            System.out.println(datasetName + "@" + dataLObjectHeaderAddress + " = " + dataObjectHeaderPrefix);

            for (HdfMessage message : dataObjectHeaderPrefix.getHeaderMessages()) {
                if (message instanceof DatatypeMessage dataTypeMessage) {
                    dataType = dataTypeMessage.getHdfDatatype();
                } else if (message instanceof DataLayoutMessage dataLayoutMessage) {
                    dataAddress = dataLayoutMessage.getDataAddress().toBigInteger().longValue();
                    dimensionSize = dataLayoutMessage.getDimensionSizes()[0].toBigInteger().longValue();
                } else if (message instanceof DataspaceMessage dataSpaceMessage) {
                    dimension = dataSpaceMessage.getDimensions()[0].toBigInteger().longValue();
                }
            }

        }

//        System.out.println("DataType{" + compoundDataType + "\r\n}");

        System.out.println("Parsing complete. NEXT: " + fileChannel.position());
    }
}
