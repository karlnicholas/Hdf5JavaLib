package org.hdf5javalib;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.infrastructure.*;
import org.hdf5javalib.file.metadata.HdfSuperblock;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
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
            globalHeap.readFromFileChannel(fileChannel, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HdfFileReader readFile() throws IOException {
        superblock = HdfSuperblock.readFromFileChannel(fileChannel, this);
        log.debug("{}", superblock);

        long objectHeaderAddress = superblock.getRootGroupSymbolTableEntry().getObjectHeaderOffset().getInstance(Long.class);
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, this);

        long localHeapAddress = superblock.getRootGroupSymbolTableEntry().getLocalHeapOffset().getInstance(Long.class);
        fileChannel.position(localHeapAddress);
        HdfLocalHeap localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, this);

        long bTreeAddress = superblock.getRootGroupSymbolTableEntry().getBTreeOffset().getInstance(Long.class);
        fileChannel.position(bTreeAddress);
        HdfBTreeV1 bTree = HdfBTreeV1.readFromFileChannel(fileChannel, this);

        Map<String, HdfGroup.DataSetInfo> datasetMap = collectDatasetsMap(fileChannel, bTree, localHeap);

        rootGroup = new HdfGroup(
                null,
                "",
                objectHeader,
                bTree,
                localHeap,
                datasetMap
        );

        log.debug("{}", rootGroup);
        log.debug("Parsing complete. NEXT: {}", fileChannel.position());

        return this;
    }

    private Map<String, HdfGroup.DataSetInfo> collectDatasetsMap(SeekableByteChannel fileChannel, HdfBTreeV1 bTree, HdfLocalHeap localHeap) throws IOException {
        Map<String, HdfGroup.DataSetInfo> dataSets = new LinkedHashMap<>();
        collectDatasetsRecursive(bTree, dataSets, localHeap, fileChannel);
        return dataSets;
    }

    private void collectDatasetsRecursive(HdfBTreeV1 currentNode,
                                          Map<String, HdfGroup.DataSetInfo> dataSets,
                                          HdfLocalHeap localHeap,
                                          SeekableByteChannel fileChannel) throws IOException {
        for (HdfBTreeEntry entry : currentNode.getEntries()) {
            if (entry.isLeafEntry()) {
                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
                for (HdfSymbolTableEntry ste : snod.getSymbolTableEntries()) {
                    HdfString linkName = localHeap.parseStringAtOffset(ste.getLinkNameOffset());
                    long dataObjectHeaderAddress = ste.getObjectHeaderOffset().getInstance(Long.class);
                    long linkNameOffset = ste.getLinkNameOffset().getInstance(Long.class);
                    fileChannel.position(dataObjectHeaderAddress);
                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, this);
                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow();
                    HdfDataSet dataset = new HdfDataSet(this, linkName.toString(), dataType.getHdfDatatype(), header);
                    HdfGroup.DataSetInfo dataSetInfo = new HdfGroup.DataSetInfo(dataset,
                            HdfWriteUtils.hdfFixedPointFromValue(0, getFixedPointDatatypeForOffset()),
                            linkNameOffset);
                    dataSets.put(linkName.toString(), dataSetInfo);
                }
            } else if (entry.isInternalEntry()) {
                HdfBTreeV1 childBTree = entry.getChildBTree();
                collectDatasetsRecursive(childBTree, dataSets, localHeap, fileChannel);
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

    @Override
    public FixedPointDatatype getFixedPointDatatypeForOffset() {
        return null;
    }

    @Override
    public FixedPointDatatype getFixedPointDatatypeForLength() {
        return null;
    }

}