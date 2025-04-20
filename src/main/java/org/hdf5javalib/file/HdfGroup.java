package org.hdf5javalib.file;

import lombok.Getter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.SymbolTableMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.file.infrastructure.HdfBTreeV1;
import org.hdf5javalib.file.infrastructure.HdfGroupSymbolTableNode;
import org.hdf5javalib.file.infrastructure.HdfLocalHeap;
import org.hdf5javalib.file.infrastructure.HdfLocalHeapContents;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class HdfGroup implements Closeable {
    private final HdfFile hdfFile;
    private final String name;
    private final HdfObjectHeaderPrefixV1 objectHeader;
    private final HdfBTreeV1 bTree;
    private final HdfLocalHeap localHeap;
    private HdfLocalHeapContents localHeapContents;
    private final Map<String, DataSetInfo> dataSets;

    /**
     * Inner class to hold dataset, its header offset, and link name offset.
     */
    public static class DataSetInfo {
        private final HdfDataSet dataSet;
        private final HdfFixedPoint headerOffset;
        private final long linkNameOffset;

        public DataSetInfo(HdfDataSet dataSet, HdfFixedPoint headerOffset, long linkNameOffset) {
            this.dataSet = dataSet;
            this.headerOffset = headerOffset;
            this.linkNameOffset = linkNameOffset;
        }

        public HdfDataSet getDataSet() {
            return dataSet;
        }

        public HdfFixedPoint getHeaderOffset() {
            return headerOffset;
        }

        public long getLinkNameOffset() {
            return linkNameOffset;
        }
    }

    /**
     * Constructs an HdfGroup for reading an existing HDF5 file.
     * Initializes the group with metadata and datasets parsed from the file.
     *
     * @param hdfFile the HDF5 file containing this group
     * @param name the name of the group
     * @param objectHeader the object header prefix containing group metadata
     * @param bTree the B-tree managing symbol table entries
     * @param localHeap the local heap storing link names
     * @param localHeapContents the contents of the local heap
     * @param dataSets a map of dataset names to their corresponding HdfDataSet objects
     */
    public HdfGroup(
            HdfFile hdfFile,
            String name,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap,
            HdfLocalHeapContents localHeapContents,
            Map<String, DataSetInfo> dataSets
    ) {
        this.hdfFile = hdfFile;
        this.name = name;
        this.objectHeader = objectHeader;
        this.bTree = bTree;
        this.localHeap = localHeap;
        this.localHeapContents = localHeapContents;
        this.dataSets = dataSets;
    }

    /**
     * Constructs an HdfGroup for creating a new HDF5 file to be written.
     * Initializes the group with a new B-tree, local heap, and empty dataset map.
     *
     * @param hdfFile the HDF5 file to be written
     * @param name the name of the group
     * @param btreeAddress the file address for the B-tree
     * @param localHeapAddress the file address for the local heap
     */
    public HdfGroup(HdfFile hdfFile, String name, long btreeAddress, long localHeapAddress) {
        HdfFileAllocation fileAllocation = hdfFile.getFileAllocation();
        this.hdfFile = hdfFile;
        this.name = name;
        long localHeapContentsSize = fileAllocation.getCurrentLocalHeapContentsSize();
        byte[] heapData = new byte[(int) localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;

        localHeap = new HdfLocalHeap(HdfFixedPoint.of(localHeapContentsSize), HdfFixedPoint.of(fileAllocation.getCurrentLocalHeapContentsOffset()), hdfFile);
        localHeapContents = new HdfLocalHeapContents(heapData);
        localHeap.addToHeap(
                new HdfString(new byte[0], new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), 0))
                , localHeapContents
        );

        bTree = new HdfBTreeV1("TREE", 0, 0,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8),
                hdfFile);

        HdfFixedPoint btree = HdfFixedPoint.of(btreeAddress);
        HdfFixedPoint localHeap = HdfFixedPoint.of(localHeapAddress);

        objectHeader = new HdfObjectHeaderPrefixV1(1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(btree, localHeap, (byte)0, (short) (btree.getDatatype().getSize() + localHeap.getDatatype().getSize()))));

        this.dataSets = new LinkedHashMap<>();
    }

    public HdfDataSet createDataSet(HdfDataFile hdfDataFile, String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        DatasetAllocationInfo allocationInfo = fileAllocation.allocateDatasetStorage(datasetName);

        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), datasetName.getBytes().length));
        HdfDataSet newDataSet = new HdfDataSet(hdfDataFile, datasetName, hdfDatatype, dataSpaceMessage);

        HdfLocalHeap.Pair<HdfLocalHeapContents, Integer> updateHeapResult = localHeap.addToHeap(hdfDatasetName, localHeapContents);
        this.localHeapContents = updateHeapResult.getFirst();

        DataSetInfo dataSetInfo = new DataSetInfo(newDataSet, HdfFixedPoint.of(allocationInfo.getHeaderOffset()), updateHeapResult.getSecond().longValue());
        dataSets.put(datasetName, dataSetInfo);

        bTree.addDataset(updateHeapResult.getSecond(), allocationInfo.getHeaderOffset(), datasetName);

        return newDataSet;
    }

    /**
     * Retrieves the dataset name associated with a given linkNameOffset.
     *
     * @param linkNameOffset The link name offset to look up.
     * @return The dataset name, or null if not found.
     */
    public String getDatasetNameByLinkNameOffset(long linkNameOffset) {
        for (Map.Entry<String, DataSetInfo> entry : dataSets.entrySet()) {
            if (entry.getValue().getLinkNameOffset() == linkNameOffset) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("Link name offset " + linkNameOffset + " not found in group " + name);
    }

    public void writeToFileChannel(SeekableByteChannel seekableByteChannel) throws IOException {
        HdfFileAllocation fileAllocation = hdfFile.getFileAllocation();
        objectHeader.writeAsGroupToByteChannel(seekableByteChannel, fileAllocation);
        bTree.writeToByteChannel(seekableByteChannel, fileAllocation);
        localHeap.writeToByteChannel(seekableByteChannel);
        localHeapContents.writeToByteChannel(seekableByteChannel, fileAllocation);

        Map<Long, HdfGroupSymbolTableNode> mapOffsetToSnod = bTree.mapOffsetToSnod();
        ByteBuffer snodBuffer = ByteBuffer.allocate((int)HdfFileAllocation.getSNOD_STORAGE_SIZE()).order(ByteOrder.LITTLE_ENDIAN);
        for (Map.Entry<Long, HdfGroupSymbolTableNode> offsetAndStn : mapOffsetToSnod.entrySet()) {
            offsetAndStn.getValue().writeToBuffer(snodBuffer);
            snodBuffer.rewind();
            seekableByteChannel.position(offsetAndStn.getKey());
            while (snodBuffer.hasRemaining()) {
                seekableByteChannel.write(snodBuffer);
            }
            snodBuffer.clear();
        }
    }

    public Collection<HdfDataSet> getDataSets() {
        return dataSets.values().stream().map(DataSetInfo::getDataSet).collect(Collectors.toList());
    }

    public HdfDataSet findDataset(String datasetName) {
        return dataSets.get(datasetName).getDataSet();
    }

    @Override
    public String toString() {
        String dataSetsString = dataSets.isEmpty()
                ? ""
                : "\r\ndataSets=[\r\n" + dataSets.entrySet().stream()
                .map(entry -> "  " + entry.getKey() + "@" + entry.getValue().getHeaderOffset() + " " + entry.getValue().getDataSet().getDataObjectHeaderPrefix())
                .collect(Collectors.joining(",\r\n")) + "\r\n]";

        return "HdfGroup{" +
                "name='" + name + '\'' +
                "\r\nobjectHeader=" + objectHeader +
                "\r\nbTree=" + bTree +
                "\r\nlocalHeap=" + localHeap +
                "\r\nlocalHeapContents=" + localHeapContents +
                dataSetsString +
                "}";
    }

    @Override
    public void close() throws IOException {
        for (DataSetInfo datasetInfo : dataSets.values()) {
            datasetInfo.getDataSet().close();
        }
    }
}