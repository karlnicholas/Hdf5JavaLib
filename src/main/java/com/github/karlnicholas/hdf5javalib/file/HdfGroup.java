package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.data.HdfString;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.message.SymbolTableMessage;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

@Getter
public class HdfGroup {
    private final HdfFile hdfFile;
//    private final HdfSymbolTableEntry symbolTableEntry;
//    private final HdfObjectHeaderPrefixV1 objectHeader;
//    private final HdfLocalHeap localHeap;
//    private final HdfLocalHeapContents localHeapContents;
//    private final HdfBTreeV1 bTree;
    private final HdfGroupSymbolTableNode symbolTableNode;
//    private int localHeapContentsSize;
    private final String name;

    public HdfGroup(
            HdfFile hdfFile,
//            HdfSymbolTableEntry symbolTableEntry,
//            HdfObjectHeaderPrefixV1 objectHeader,
//            HdfLocalHeap localHeap,
//            HdfLocalHeapContents localHeapContents,
//            HdfBTreeV1 bTree,
            HdfGroupSymbolTableNode symbolTableNode,
            String name
    ) {
        this.hdfFile = hdfFile;
//        this.symbolTableEntry = symbolTableEntry;
//        this.objectHeader = objectHeader;
//        this.localHeap = localHeap;
//        this.localHeapContents = localHeapContents;
//        this.bTree = bTree;
        this.symbolTableNode = symbolTableNode;
        this.name = name;
    }

    public HdfGroup(HdfFile hdfFile, String name) {
        this.hdfFile = hdfFile;
        this.name = name;
//    private final HdfObjectHeaderPrefixV1 objectHeader;
//    private final HdfLocalHeap localHeap;
//    private final HdfLocalHeapContents localHeapContents;
//    private final HdfBTreeV1 bTree;
//    private final HdfGroupSymbolTableNode symbolTableNode;
        int localHeapContentsSize;
        // Define the heap data size, why 88 I don't know.
        // Initialize the heapData array
        localHeapContentsSize = 88;
        byte[] heapData = new byte[localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;

        HdfLocalHeap localHeap = new HdfLocalHeap(HdfFixedPoint.of(localHeapContentsSize), HdfFixedPoint.undefined((short)8));
        HdfLocalHeapContents localHeapContents = new HdfLocalHeapContents(heapData);
        localHeap.addToHeap(new HdfString(new byte[0], false, false), localHeapContents);

        // Define a B-Tree for group indexing
        HdfBTreeV1 bTree = new HdfBTreeV1("TREE", 0, 0, 0,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8));

        HdfObjectHeaderPrefixV1 objectHeader = new HdfObjectHeaderPrefixV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.undefined((short)8),
                        HdfFixedPoint.undefined((short)8))));

        // Define a root group
        HdfSymbolTableEntry symbolTableEntry = new HdfSymbolTableEntry(
                HdfFixedPoint.of(0),
                objectHeader,
                bTree,
                localHeap,
                localHeapContents);


        symbolTableNode = new HdfGroupSymbolTableNode("SNOD", 1, 0, List.of(symbolTableEntry));
    }


//    public HdfDataSet createDataSet(String datasetName, CompoundDatatype compoundType) {
//        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), false, false);
//        // real steps needed to add a group.
//        // entry in btree = "Demand" + snodOffset (1880)
//        // entry in locaheapcontents = "Demand" = datasetName
//        int linkNameOffset = bTree.addGroup(hdfDatasetName, HdfFixedPoint.undefined((short)8), localHeap, localHeapContents);
//        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
//                HdfFixedPoint.of(linkNameOffset),
//                objectHeader,
//                bTree,
//                localHeap
//        );
//        symbolTableNode.addEntry(ste);
//        // entry in snod = linkNameOffset=8, objectHeaderAddress=800, cacheType=0,
//        return new HdfDataSet(this, datasetName, compoundType, HdfFixedPoint.undefined((short)0));
//    }
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

    public void write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
        hdfFile.write(bufferSupplier, hdfDataSet);
    }
    @Override
    public String toString() {
        return "HdfGroup{" +
                "name='" + name + '\'' +
                "\r\n\tsymbolTableNode=" + symbolTableNode +
                '}';
    }
}
