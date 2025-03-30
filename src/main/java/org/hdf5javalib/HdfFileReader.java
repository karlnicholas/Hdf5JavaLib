package org.hdf5javalib;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.*;
import org.hdf5javalib.file.metadata.HdfSuperblock;

import java.io.IOException;
import java.nio.channels.FileChannel;

@Getter
@Slf4j
public class HdfFileReader {
    // level 0
    private HdfSuperblock superblock;
    // level 1
    private HdfGroup rootGroup;
//    // level 2A1
//    // level 2A1
//    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;
//    // parsed Datatype
//    private HdfDatatype dataType;
//    private long dataAddress = 0;
//    private long dimensionSize = 0;
//    private long dimension = 0;

    public void readFile(FileChannel fileChannel) throws IOException {
        // Parse the superblock at the beginning of the file
        superblock = HdfSuperblock.readFromFileChannel(fileChannel);
        log.debug("{}", superblock);

        short offsetSize = superblock.getOffsetSize();
        short lengthSize = superblock.getLengthSize();
//
//        rootSymbolTableEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
//
        // Get the object header address from the superblock
        // Parse the object header from the file using the superblock information
        long objectHeaderAddress = superblock.getRootGroupSymbolTableEntry().getObjectHeaderAddress().getInstance(Long.class);
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize);

        // Parse the local heap using the file channel
        // Read data from file channel starting at the specified position
        long localHeapAddress = superblock.getRootGroupSymbolTableEntry().getNameHeapAddress().getInstance(Long.class);
        fileChannel.position(localHeapAddress);
        HdfLocalHeap localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());

        long dataSize = localHeap.getDataSegmentSize().getInstance(Long.class);
        long dataSegmentAddress = localHeap.getDataSegmentAddress().getInstance(Long.class);
        fileChannel.position(dataSegmentAddress);
        HdfLocalHeapContents localHeapContents = HdfLocalHeapContents.readFromFileChannel(fileChannel, (int) dataSize);

        long bTreeAddress = superblock.getRootGroupSymbolTableEntry().getBTreeAddress().getInstance(Long.class);
        fileChannel.position(bTreeAddress);
        HdfBTreeV1 bTree = HdfBTreeV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());

        rootGroup = new HdfGroup(
                null,
                "",
                objectHeader,
                bTree,
                localHeap,
                localHeapContents
        );


        log.debug("{}", rootGroup);

        log.debug("Parsing complete. NEXT: {}", fileChannel.position());
    }

    public HdfDataSet findDataset(String targetName, FileChannel fileChannel, HdfGroup hdfGroup) throws IOException {
        for (HdfBTreeEntry entry : hdfGroup.getBTree().getEntries()) {
            HdfGroupSymbolTableNode symbolTableNode = entry.getSymbolTableNode();
            for (HdfSymbolTableEntry ste : symbolTableNode.getSymbolTableEntries()) {
                HdfString datasetName = hdfGroup.getLocalHeapContents().parseStringAtOffset(ste.getLinkNameOffset());
                if (datasetName.toString().equals(targetName)) {
                    long dataObjectHeaderAddress = ste.getObjectHeaderAddress().getInstance(Long.class);
                    fileChannel.position(dataObjectHeaderAddress);
                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize());
                    // Assuming a way to check if it’s a dataset (e.g., header type field)
                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow();
                    return new HdfDataSet(rootGroup, datasetName.toString(), dataType.getHdfDatatype(), header);
                }
            }
        }
        throw new IllegalArgumentException("No such dataset: " + targetName);
    }
}
