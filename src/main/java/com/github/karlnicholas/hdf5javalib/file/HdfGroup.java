package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.data.HdfString;
import com.github.karlnicholas.hdf5javalib.datatype.HdfDatatype;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.message.DataspaceMessage;
import com.github.karlnicholas.hdf5javalib.message.SymbolTableMessage;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Supplier;

@Getter
public class HdfGroup {
    private final String name;
    private final HdfSymbolTableEntry symbolTableEntry;
    private final HdfObjectHeaderPrefixV1 objectHeader;
    private final HdfBTreeV1 bTree;
    private final HdfLocalHeap localHeap;
    private final HdfLocalHeapContents localHeapContents;
    private final HdfGroupSymbolTableNode symbolTableNode;
//    private int localHeapContentsSize;

    public HdfGroup(
            String name,
            HdfSymbolTableEntry symbolTableEntry,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap,
            HdfLocalHeapContents localHeapContents,
            HdfGroupSymbolTableNode symbolTableNode
    ) {
        this.name = name;
        this.symbolTableEntry = symbolTableEntry;
        this.objectHeader = objectHeader;
        this.bTree = bTree;
        this.localHeap = localHeap;
        this.localHeapContents = localHeapContents;
        this.symbolTableNode = symbolTableNode;
    }

    public HdfGroup(String name, HdfSymbolTableEntry symbolTableEntry) {
        this.name = name;
        int localHeapContentsSize;
        // Define the heap data size, why 88 I don't know.
        // Initialize the heapData array
        localHeapContentsSize = 88;
        byte[] heapData = new byte[localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;

        localHeap = new HdfLocalHeap(HdfFixedPoint.of(localHeapContentsSize), HdfFixedPoint.undefined((short)8));
        localHeapContents = new HdfLocalHeapContents(heapData);
        localHeap.addToHeap(new HdfString(new byte[0], false, false), localHeapContents);

        // Define a B-Tree for group indexing
        bTree = new HdfBTreeV1("TREE", 0, 0, 0,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8));

        objectHeader = new HdfObjectHeaderPrefixV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.undefined((short)8),
                        HdfFixedPoint.undefined((short)8))));

        // Define a root group
        this.symbolTableEntry = symbolTableEntry;
        symbolTableNode = new HdfGroupSymbolTableNode("SNOD", 1, 0, new ArrayList<>());
    }


    public HdfDataSet createDataSet(String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), false, false);
        // real steps needed to add a group.
        // entry in btree = "Demand" + snodOffset (1880)
        // entry in locaheapcontents = "Demand" = datasetName
        int linkNameOffset = bTree.addGroup(hdfDatasetName, HdfFixedPoint.undefined((short)8),
                        localHeap,
                        localHeapContents);
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
                HdfFixedPoint.of(linkNameOffset),
                // not correct?
                symbolTableEntry.getObjectHeaderAddress(),
                0,
                null,
                null
        );
        symbolTableNode.addEntry(ste);
        return new HdfDataSet(this, datasetName, hdfDatatype, HdfFixedPoint.undefined((short)0));
    }
//
//    public ByteBuffer close(ByteBuffer buffer) {
////        System.out.println(symbolTableEntry);
//        // Write the root group symbol table entry immediately after the superblock
////        symbolTableEntry.writeToByteBuffer(buffer, hdfFile.getSuperblock().getSizeOfOffsets());
//
//        System.out.println(objectHeader);
//        // Write Object Header at position found in rootGroupEntry
//        int objectHeaderAddress = hdfFile.getObjectHeaderPrefixAddress();
//        buffer.position(objectHeaderAddress);
//        objectHeader.writeToByteBuffer(buffer);
//
//        long localHeapPosition = -1;
//        long bTreePosition = -1;
//
//        // Try getting the Local Heap Address from the Root Symbol Table Entry
//        if (hdfFile.getLocalHeapAddress() > 0) {
//            localHeapPosition = hdfFile.getLocalHeapAddress();
//        }
//
//        // If not found or invalid, fallback to Object Header's SymbolTableMessage
//        Optional<SymbolTableMessage> symbolTableMessageOpt = objectHeader.findHdfSymbolTableMessage(SymbolTableMessage.class);
//        if (symbolTableMessageOpt.isPresent()) {
//            SymbolTableMessage symbolTableMessage = symbolTableMessageOpt.get();
//
//            // Retrieve Local Heap Address if still not found
//            if (localHeapPosition == -1 && symbolTableMessage.getLocalHeapAddress() != null && !symbolTableMessage.getLocalHeapAddress().isUndefined()) {
//                localHeapPosition = symbolTableMessage.getLocalHeapAddress().getBigIntegerValue().longValue();
//            }
//
//            // Retrieve B-Tree Address
//            if (symbolTableMessage.getBTreeAddress() != null && !symbolTableMessage.getBTreeAddress().isUndefined()) {
//                bTreePosition = symbolTableMessage.getBTreeAddress().getBigIntegerValue().longValue();
//            }
//        }
//
//        // Validate B-Tree Position and write it
//        if (bTreePosition != -1) {
//            System.out.println(bTree);
//            buffer.position((int) bTreePosition); // Move to the correct position
//            bTree.writeToByteBuffer(buffer);
//        } else {
//            throw new IllegalStateException("No valid B-Tree position found.");
//        }
//
//        // Validate Local Heap Position and write it
//        if (localHeapPosition != -1) {
//            buffer.position((int) localHeapPosition); // Move to the correct position
//            localHeap.writeToByteBuffer(buffer);
//            buffer.position(localHeap.getDataSegmentAddress().getBigIntegerValue().intValue());
//            localHeapContents.writeToByteBuffer(buffer);
//        } else {
//            throw new IllegalStateException("No valid Local Heap position found.");
//        }
//        return buffer.flip();
//    }

    public long write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
//        hdfFile.write(bufferSupplier, hdfDataSet);
        return 0;
    }
    @Override
    public String toString() {
        return "HdfGroup{" +
                "name='" + name + '\'' +
                "\r\n\tsymbolTableEntry=" + symbolTableEntry +
                "\r\n\tobjectHeader=" + objectHeader +
                "\r\n\tbTree=" + bTree +
                "\r\n\tlocalHeap=" + localHeap +
                "\r\n\tlocalHeapContents=" + localHeapContents +
                "\r\n\tsymbolTableNode=" + symbolTableNode +
                "}";
    }
}
