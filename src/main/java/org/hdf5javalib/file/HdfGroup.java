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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;

@Getter
public class HdfGroup {
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

    public void writeToBuffer(ByteBuffer buffer) {
        // Write Object Header at position found in rootGroupEntry
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        long objectHeaderPrefixAddress = fileAllocation.getObjectHeaderPrefixOffset();

        buffer.position((int) objectHeaderPrefixAddress);
        objectHeader.writeToByteBuffer(buffer);

        long localHeapPosition = -1;
        long bTreePosition = -1;

        // Try getting the Local Heap Address from the Root Symbol Table Entry
        if (fileAllocation.getLocalHeapOffset() > 0) {
            localHeapPosition = fileAllocation.getLocalHeapOffset();
        }

        // If not found or invalid, fallback to Object Header's SymbolTableMessage
        Optional<SymbolTableMessage> symbolTableMessageOpt = objectHeader.findMessageByType(SymbolTableMessage.class);
        if (symbolTableMessageOpt.isPresent()) {
            SymbolTableMessage symbolTableMessage = symbolTableMessageOpt.get();

            // Retrieve Local Heap Address if still not found
            if (localHeapPosition == -1 && symbolTableMessage.getLocalHeapAddress() != null && !symbolTableMessage.getLocalHeapAddress().isUndefined()) {
                localHeapPosition = symbolTableMessage.getLocalHeapAddress().getInstance(Long.class);
            }

            // Retrieve B-Tree Address
            if (symbolTableMessage.getBTreeAddress() != null && !symbolTableMessage.getBTreeAddress().isUndefined()) {
                bTreePosition = symbolTableMessage.getBTreeAddress().getInstance(Long.class);
            }
        }

        // Validate B-Tree Position and write it
        if (bTreePosition != -1) {
            buffer.position((int) bTreePosition); // Move to the correct position
            bTree.writeToByteBuffer(buffer);
        } else {
            throw new IllegalStateException("No valid B-Tree position found.");
        }

        // Validate Local Heap Position and write it
        if (localHeapPosition != -1) {
            buffer.position((int) localHeapPosition); // Move to the correct position
            localHeap.writeToByteBuffer(buffer);
            buffer.position(localHeap.getHeapContentsOffset().getInstance(Long.class).intValue());
            localHeapContents.writeToByteBuffer(buffer);
        } else {
            throw new IllegalStateException("No valid Local Heap position found.");
        }

        // need to write the dataset
        if ( dataSet != null ) {
            DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(dataSet.getDatasetName());
            buffer.position((int) allocationInfo.getHeaderOffset());
            dataSet.writeToBuffer(buffer);
        }

    }

    public long write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
        return hdfFile.write(bufferSupplier);
    }
    public long write(ByteBuffer byteBuffer, HdfDataSet hdfDataSet) throws IOException {
        return hdfFile.write(byteBuffer,  hdfDataSet);
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

}
