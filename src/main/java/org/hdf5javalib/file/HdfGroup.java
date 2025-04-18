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
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Getter
public class HdfGroup implements Closeable {
    private final HdfFile hdfFile;
    private final String name;
    private final HdfObjectHeaderPrefixV1 objectHeader;
    private final HdfBTreeV1 bTree;
    private final HdfLocalHeap localHeap;
    private final HdfLocalHeapContents localHeapContents;
    private final Map<String, HdfDataSet> dataSets;

    /**
     * Used when HDF file is being read.
     * @param hdfFile
     * @param name
     * @param objectHeader
     * @param bTree
     * @param localHeap
     * @param localHeapContents
     * @param dataSets
     */
    public HdfGroup(
            HdfFile hdfFile,
            String name,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap,
            HdfLocalHeapContents localHeapContents,
            Map<String, HdfDataSet> dataSets
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
     * For creating a new HDF file to be written.
     * @param hdfFile
     * @param name
     * @param btreeAddress
     * @param localHeapAddress
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
        localHeapContents = new HdfLocalHeapContents(heapData, hdfFile);
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
        HdfFileAllocation fileAllocation = hdfFile.getFileAllocation();
        DatasetAllocationInfo allocationInfo = fileAllocation.allocateDatasetStorage(datasetName);

        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), datasetName.getBytes().length));
        HdfDataSet newDataSet = new HdfDataSet(hdfDataFile, datasetName, hdfDatatype, dataSpaceMessage);
        dataSets.put(datasetName, newDataSet);

        int linkNameOffset = localHeap.addToHeap(hdfDatasetName, localHeapContents);
        bTree.addDataset(linkNameOffset, allocationInfo.getHeaderOffset());

        return newDataSet;
    }

    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
        HdfFileAllocation fileAllocation = hdfFile.getFileAllocation();
        long rootGroupSize = fileAllocation.getRootGroupSize();
        long rootGroupOffset = fileAllocation.getRootGroupOffset();
        ByteBuffer buffer = ByteBuffer.allocate((int) rootGroupSize).order(ByteOrder.LITTLE_ENDIAN);
        objectHeader.writeToBuffer(buffer);
        bTree.writeToByteBuffer(buffer);
        localHeap.writeToByteBuffer(buffer);
        localHeapContents.writeToByteBuffer(buffer);
        buffer.rewind();

        fileChannel.position(rootGroupOffset);
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        Map<Long, HdfGroupSymbolTableNode> mapOffsetToSnod = bTree.mapOffsetToSnod();
        ByteBuffer snodBuffer = ByteBuffer.allocate((int)HdfFileAllocation.getSNOD_STORAGE_SIZE()).order(ByteOrder.LITTLE_ENDIAN);
        for (Map.Entry<Long, HdfGroupSymbolTableNode> offsetAndStn : mapOffsetToSnod.entrySet()) {
            offsetAndStn.getValue().writeToBuffer(snodBuffer);
            snodBuffer.rewind();
            fileChannel.position(offsetAndStn.getKey());
            while (snodBuffer.hasRemaining()) {
                fileChannel.write(snodBuffer);
            }
            snodBuffer.clear();
        }
    }

//    public void write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
//        hdfFile.write(bufferSupplier, hdfDataSet);
//    }
//
//    public void write(ByteBuffer byteBuffer, HdfDataSet hdfDataSet) throws IOException {
//        hdfFile.write(byteBuffer, hdfDataSet);
//    }

    public Collection<HdfDataSet> getDataSets() {
        return dataSets.values();
    }

    public HdfDataSet findDataset(String datasetName) {
        return dataSets.get(datasetName);
    }

    @Override
    public String toString() {
        String dataSetsString = dataSets.isEmpty()
                ? ""
                : "\r\ndataSets=[\r\n" + dataSets.entrySet().stream()
                .map(entry -> "  " + entry.getKey() + ": " + entry.getValue().getDataObjectHeaderPrefix())
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
        for (HdfDataSet dataSet : dataSets.values()) {
            if (dataSet != null) {
                dataSet.close();
            }
        }
    }

}