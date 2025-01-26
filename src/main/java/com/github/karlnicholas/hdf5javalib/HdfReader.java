package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.message.ContinuationMessage;
import com.github.karlnicholas.hdf5javalib.message.DataTypeMessage;
import com.github.karlnicholas.hdf5javalib.message.HdfMessage;
import com.github.karlnicholas.hdf5javalib.utils.BtreeV1GroupNode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.*;

public class HdfReader {
    private final File file;
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
    CompoundDataType compoundDataType;

    public HdfReader(String filePath) {
        this.file = new File(filePath);
    }

    public void readFile() throws IOException {
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel fileChannel = fis.getChannel()) {

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

            BtreeV1GroupNode btreeV1GroupNode = parseBTreeAndLocalHeap(bTree, localHeapContents);
            System.out.println(btreeV1GroupNode);

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
                }
            }
            System.out.println("DataType{" + compoundDataType + "\r\n}");
//            parseDataHeaderV1(buffer);
//            parseDataHeaderV1(buffer);
//
//            if (isNextSignature(buffer, "SNOD")) {
//                parseSymbolTableNode(buffer);
//            }

            System.out.println("Parsing complete. NEXT: " + fileChannel.position());
        }
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

}
