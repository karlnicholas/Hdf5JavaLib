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
import java.util.ArrayList;
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
    private final HdfGroupSymbolTableNode symbolTableNode;
    private HdfDataSet dataSet;

    public HdfGroup(
            HdfFile hdfFile,
            String name,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap,
            HdfLocalHeapContents localHeapContents,
            HdfGroupSymbolTableNode symbolTableNode
    ) {
        this.hdfFile = hdfFile;
        this.name = name;
        this.objectHeader = objectHeader;
        this.bTree = bTree;
        this.localHeap = localHeap;
        this.localHeapContents = localHeapContents;
        this.symbolTableNode = symbolTableNode;
    }

    public HdfGroup(HdfFile hdfFile, String name, long btreeAddress, long localHeapAddress) {
        this.hdfFile = hdfFile;
        this.name = name;
        int localHeapContentsSize;
        // Define the heap data size, why 88 I don't know.
        // Initialize the heapData array
        localHeapContentsSize = 88;
        byte[] heapData = new byte[localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;

        localHeap = new HdfLocalHeap(HdfFixedPoint.of(localHeapContentsSize), HdfFixedPoint.of(hdfFile.getBufferAllocation().getLocalHeapContentsAddress()));
        localHeapContents = new HdfLocalHeapContents(heapData);
        localHeap.addToHeap(new HdfString(new byte[0], new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), 0)), localHeapContents);

        // Define a B-Tree for group indexing
        bTree = new HdfBTreeV1("TREE", 0, 0, 0,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8));

        HdfFixedPoint btree = HdfFixedPoint.of(btreeAddress);
        HdfFixedPoint localHeap = HdfFixedPoint.of(localHeapAddress);

        // (short) (bTreeAddress.getDatatype().getSize() + localHeapAddress.getDatatype().getSize()
        objectHeader = new HdfObjectHeaderPrefixV1(1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(btree, localHeap, (byte)0,(short) (btree.getDatatype().getSize() + localHeap.getDatatype().getSize()))));

        // Define a root group
        symbolTableNode = new HdfGroupSymbolTableNode("SNOD", 1, 0, new ArrayList<>());
    }


    public HdfDataSet createDataSet(String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage, long objectHeaderAddress) {
        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), datasetName.getBytes().length));
        // this poosibly changes addresses for anything after the dataGroupAddress, which includes the SNOD address.
        dataSet = new HdfDataSet(this, datasetName, hdfDatatype, dataSpaceMessage);
        int linkNameOffset = bTree.addGroup(hdfDatasetName, HdfFixedPoint.of(hdfFile.getBufferAllocation().getSnodAddress()),
                localHeap,
                localHeapContents);
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
                HdfFixedPoint.of(linkNameOffset),
                HdfFixedPoint.of(objectHeaderAddress));
        symbolTableNode.addEntry(ste);
        return dataSet;
    }

    public void writeToBuffer(ByteBuffer buffer) {
        // Write Object Header at position found in rootGroupEntry
        long dataGroupAddress = hdfFile.getBufferAllocation().getObjectHeaderPrefixAddress();
        buffer.position((int) dataGroupAddress);
        objectHeader.writeToByteBuffer(buffer);

        long localHeapPosition = -1;
        long bTreePosition = -1;

        // Try getting the Local Heap Address from the Root Symbol Table Entry
        if (hdfFile.getBufferAllocation().getLocalHeapAddress() > 0) {
            localHeapPosition = hdfFile.getBufferAllocation().getLocalHeapAddress();
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
            buffer.position(localHeap.getDataSegmentAddress().getInstance(Long.class).intValue());
            localHeapContents.writeToByteBuffer(buffer);
        } else {
            throw new IllegalStateException("No valid Local Heap position found.");
        }

        // need to writre the dataset
        if ( dataSet != null ) {
            buffer.position((int) hdfFile.getBufferAllocation().getDataGroupAddress());
            dataSet.writeToBuffer(buffer);
        }

        buffer.position((int) hdfFile.getBufferAllocation().getSnodAddress());
        symbolTableNode.writeToBuffer(buffer);

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
                "\r\n\tobjectHeader=" + objectHeader +
                "\r\n\tbTree=" + bTree +
                "\r\n\tlocalHeap=" + localHeap +
                "\r\n\tlocalHeapContents=" + localHeapContents +
                "\r\n\tsymbolTableNode=" + symbolTableNode +
                (dataSet != null ? "\r\n\tdataSet=" + dataSet.getDataObjectHeaderPrefix() : "") +
                "}";
    }

}
