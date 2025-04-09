package org.hdf5javalib.file;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.SymbolTableMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.file.infrastructure.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Getter
public class HdfGroup implements Closeable {
    private final HdfFile hdfFile;
    private final String name;
    private final HdfObjectHeaderPrefixV1 objectHeader;
    private final HdfBTreeV1 bTree;
    private final HdfLocalHeap localHeap;
    private final HdfLocalHeapContents localHeapContents;
    private HdfDataSet dataSet;

    public HdfGroup(
            HdfFile hdfFile,
            String name,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap,
            HdfLocalHeapContents localHeapContents
    ) {
        this.hdfFile = hdfFile;
        this.name = name;
        this.objectHeader = objectHeader;
        this.bTree = bTree;
        this.localHeap = localHeap;
        this.localHeapContents = localHeapContents;
    }

    public HdfGroup(HdfFile hdfFile, String name, long btreeAddress, long localHeapAddress) {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        this.hdfFile = hdfFile;
        this.name = name;
        long localHeapContentsSize = fileAllocation.getCurrentLocalHeapContentsSize();
        byte[] heapData = new byte[(int) localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;

        localHeap = new HdfLocalHeap(HdfFixedPoint.of(localHeapContentsSize), HdfFixedPoint.of(fileAllocation.getCurrentLocalHeapContentsOffset()));
        localHeapContents = new HdfLocalHeapContents(heapData);
        localHeap.addToHeap(
                new HdfString(new byte[0], new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), 0))
                , localHeapContents
        );

        // Define a B-Tree for group indexing
        bTree = new HdfBTreeV1("TREE", 0, 0,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8));

        HdfFixedPoint btree = HdfFixedPoint.of(btreeAddress);
        HdfFixedPoint localHeap = HdfFixedPoint.of(localHeapAddress);

        // (short) (bTreeAddress.getDatatype().getSize() + localHeapAddress.getDatatype().getSize()
        // this is not the dataset objectHeader, its for the group to keep a SymbolTableMessage
        // a SymbolTableMessage with bree and localheap locations.
        objectHeader = new HdfObjectHeaderPrefixV1(1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(btree, localHeap, (byte)0,(short) (btree.getDatatype().getSize() + localHeap.getDatatype().getSize()))));

    }

    public HdfDataSet createDataSet(String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        DatasetAllocationInfo allocationInfo = fileAllocation.allocateDatasetStorage(datasetName);

        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), datasetName.getBytes().length));
        // this possibly changes addresses for anything after the dataGroupAddress, which includes the SNOD address.
        dataSet = new HdfDataSet(this, datasetName, hdfDatatype, dataSpaceMessage);

        // add the datasetName into the localHeap for this group
        int linkNameOffset = localHeap.addToHeap(hdfDatasetName, localHeapContents);
        //
        bTree.addDataset(linkNameOffset, allocationInfo.getHeaderOffset());

        return dataSet;
    }

    public void writeToFileChannel(FileChannel fileChannel) throws IOException {

        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        long rootGroupSize = fileAllocation.getRootGroupSize(); // Returns 704
        long rootGroupOffset = fileAllocation.getRootGroupOffset(); // Returns 96
        ByteBuffer buffer = ByteBuffer.allocate((int) rootGroupSize);
        objectHeader.writeToBuffer(buffer);  // Writes 40 bytes (96-135)
        bTree.writeToByteBuffer(buffer);        // Writes 544 bytes (136-679)
        localHeap.writeToByteBuffer(buffer);    // Writes 32 bytes (680-711)
        localHeapContents.writeToByteBuffer(buffer); // Writes 88 bytes (712-799)
        buffer.rewind();

        fileChannel.position(rootGroupOffset);
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        // need to write the dataset
        if ( dataSet != null ) {
            dataSet.writeToFileChannel(fileChannel);
        }

        // write SNOD for group.

        Map<Long, HdfGroupSymbolTableNode> mapOffsetToSnod = bTree.mapOffsetToSnod();
        ByteBuffer snodBuffer = ByteBuffer.allocate((int)HdfFileAllocation.getSNOD_STORAGE_SIZE());
        for (Map.Entry<Long, HdfGroupSymbolTableNode> offsetAndStn: mapOffsetToSnod.entrySet()) {
            offsetAndStn.getValue().writeToBuffer(snodBuffer);
            snodBuffer.rewind();
            fileChannel.position(offsetAndStn.getKey());
            while (buffer.hasRemaining()) {
                fileChannel.write(snodBuffer);
            }
            snodBuffer.clear();
        }
    }

    public void write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
        hdfFile.write(bufferSupplier, hdfDataSet);
    }
    public void write(ByteBuffer byteBuffer, HdfDataSet hdfDataSet) throws IOException {
        hdfFile.write(byteBuffer,  hdfDataSet);
    }

    @Override
    public String toString() {
        return "HdfGroup{" +
                "name='" + name + '\'' +
                "\r\nobjectHeader=" + objectHeader +
                "\r\nbTree=" + bTree +
                "\r\nlocalHeap=" + localHeap +
                "\r\nlocalHeapContents=" + localHeapContents +
                (dataSet != null ? "\r\ndataSet=" + dataSet.getDataObjectHeaderPrefix() : "") +
                "}";
    }

    @Override
    public void close() throws IOException {
        dataSet.close();
    }
}
