package org.hdf5javalib;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.*;
import org.hdf5javalib.file.metadata.HdfSuperblock;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Getter
@Slf4j
public class HdfFileReader implements HdfDataFile {
    private HdfSuperblock superblock;
    private HdfGroup rootGroup;

    private final SeekableByteChannel fileChannel;
    private final HdfGlobalHeap globalHeap;
    private final HdfFileAllocation fileAllocation;

    public HdfFileReader(SeekableByteChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.fileAllocation = new HdfFileAllocation();
        this.globalHeap = new HdfGlobalHeap(this::initializeGlobalHeap, this);
    }

    private void initializeGlobalHeap(long offset) {
        try {
            fileChannel.position(offset);
            globalHeap.readFromFileChannel(fileChannel, (short)8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HdfFileReader readFile() throws IOException {
        superblock = HdfSuperblock.readFromFileChannel(fileChannel, this);
        log.debug("{}", superblock);

        short offsetSize = superblock.getOffsetSize();
        short lengthSize = superblock.getLengthSize();

        long objectHeaderAddress = superblock.getRootGroupSymbolTableEntry().getObjectHeaderOffset().getInstance(Long.class);
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, offsetSize, lengthSize, this);

        long localHeapAddress = superblock.getRootGroupSymbolTableEntry().getLocalHeapOffset().getInstance(Long.class);
        fileChannel.position(localHeapAddress);
        HdfLocalHeap localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize(), this);

        long dataSize = localHeap.getHeapContentsSize().getInstance(Long.class);
        long dataSegmentAddress = localHeap.getHeapContentsOffset().getInstance(Long.class);
        fileChannel.position(dataSegmentAddress);
        HdfLocalHeapContents localHeapContents = HdfLocalHeapContents.readFromFileChannel(fileChannel, (int) dataSize, this);

        long bTreeAddress = superblock.getRootGroupSymbolTableEntry().getBTreeOffset().getInstance(Long.class);
        fileChannel.position(bTreeAddress);
        HdfBTreeV1 bTree = HdfBTreeV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize(), this);

        Map<String, HdfDataSet> datasetMap = collectDatasetsMap(fileChannel, bTree, localHeapContents);

        rootGroup = new HdfGroup(
                null,
                "",
                objectHeader,
                bTree,
                localHeap,
                localHeapContents,
                datasetMap
        );

        log.debug("{}", rootGroup);
        log.debug("Parsing complete. NEXT: {}", fileChannel.position());

        return this;
    }

    private Map<String, HdfDataSet> collectDatasetsMap(SeekableByteChannel fileChannel, HdfBTreeV1 bTree, HdfLocalHeapContents heapContents) throws IOException {
        Map<String, HdfDataSet> dataSets = new LinkedHashMap<>();
        collectDatasetsRecursive(bTree, dataSets, heapContents, fileChannel);
        return dataSets;
    }

    private void collectDatasetsRecursive(HdfBTreeV1 currentNode,
                                          Map<String, HdfDataSet> dataSets,
                                          HdfLocalHeapContents heapContents,
                                          SeekableByteChannel fileChannel) throws IOException {
        for (HdfBTreeEntry entry : currentNode.getEntries()) {
            if (entry.isLeafEntry()) {
                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
                for (HdfSymbolTableEntry ste : snod.getSymbolTableEntries()) {
                    HdfString linkName = heapContents.parseStringAtOffset(ste.getLinkNameOffset());
                    long dataObjectHeaderAddress = ste.getObjectHeaderOffset().getInstance(Long.class);
                    fileChannel.position(dataObjectHeaderAddress);
                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, superblock.getOffsetSize(), superblock.getLengthSize(), this);
                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow();
                    HdfDataSet dataset = new HdfDataSet(null, linkName.toString(), dataType.getHdfDatatype(), header);
                    dataSets.put(linkName.toString(), dataset);
                }
            } else if (entry.isInternalEntry()) {
                HdfBTreeV1 childBTree = entry.getChildBTree();
                collectDatasetsRecursive(childBTree, dataSets, heapContents, fileChannel);
            }
        }
    }

    @Override
    public HdfGlobalHeap getGlobalHeap() {
        return globalHeap;
    }

    @Override
    public HdfFileAllocation getFileAllocation() {
        return fileAllocation;
    }

    @Override
    public SeekableByteChannel getSeekableByteChannel() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}