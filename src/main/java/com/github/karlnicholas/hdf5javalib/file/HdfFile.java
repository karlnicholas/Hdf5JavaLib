package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.SymbolTableMessage;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Getter
public class HdfFile {
    private final String fileName;
    private final StandardOpenOption[] openOptions;
    private final AtomicLong datasetRecordCount;
    // initial setup without Dataset
    private final HdfSuperblock superblock;
    private final HdfSymbolTableEntry rootGroupEntry;
    private final HdfObjectHeaderPrefixV1 objectHeaderPrefix;
    private final HdfLocalHeap localHeap;
    private final HdfLocalHeapContents localHeapContents;
    private final HdfBTreeV1 bTree;
    private final HdfGroupSymbolTableNode groupSymbolTableNode;

    /**
     * HDF5 File Structure - Address Offsets and Sizes
     * This section defines memory layout constants for key structures in an HDF5 file.
     * Each address is computed relative to the previous structure’s size, ensuring
     * proper navigation within the file.
     */
    // **Superblock (Starting Point)**
    // The superblock is the first structure in an HDF5 file and contains metadata
    // about file format versions, data storage, and offsets to key structures.
    private final int superblockAddress = 0;  // HDF5 file starts at byte 0
    private final int superblockSize = 56;    // Superblock size (metadata about the file)

    // **Root Group Symbol Table Entry**
    // The root group symbol table entry stores metadata for the root group.
    // It contains information about group structure, attributes, and dataset links.
    private final int rootGroupSymbolTableEntryAddress = superblockAddress + superblockSize;
    private final int rootGroupSymbolTableEntrySize = 40;

    // **Object Header Prefix (Metadata about the First Group)**
    // This section contains metadata for the first group, defining attributes,
    // storage layout, and dataset properties.
    private final int objectHeaderPrefixAddress = rootGroupSymbolTableEntryAddress + rootGroupSymbolTableEntrySize;
    private final int objectHeaderPrefixSize = 40;

    // **B-tree (Manages Group Links)**
    // The B-tree is used to efficiently organize links within the group,
    // allowing quick access to datasets and subgroups.
    private final int btreeAddress = objectHeaderPrefixAddress + objectHeaderPrefixSize;
    private final int btreeSize = 32;  // Size of a B-tree node
    private final int btreeStorageSize = 512;  // Allocated storage for B-tree nodes

    // **Local Heap (Stores Group Names & Small Objects)**
    // The local heap stores small metadata elements such as object names
    // and soft links, reducing fragmentation in the file.
    private final int localHeapAddress = btreeAddress + btreeSize + btreeStorageSize;
    private final int localHeapSize = 32;  // Header for the local heap
    private final int localHeapContentsAddress = localHeapAddress + localHeapSize;  // Contents stored inside the heap
    private final int localHeapContentsSize = 88;  // Contents stored inside the heap

    // **First Group Address (Computed from Previous Structures)**
    // This is the byte offset where the first group starts in the file.
    // It is calculated based on the sum of all preceding metadata structures.
    private final int firstGroupAddress = localHeapContentsAddress + localHeapContentsSize;
    private final int firstGroupStorageSize = 16 + 1064;  // Total size of the first group's metadata

    // **Symbol Table Node (SNOD)**
    // The SNOD (Symbol Table Node) organizes entries for objects within the group.
    // It manages links to datasets, other groups, and named datatypes.
    private final int snodAddress = firstGroupAddress + firstGroupStorageSize;
    private final int snodSize = 8;  // Header or control structure for the SNOD

    // **SNOD Entry (Represents Objects in the Group)**
    // An SNOD entry describes individual datasets or subgroups within the group.
    // Each entry in the SNOD table includes the object name, address, and type.
    private final int snodEntrySize = 32;  // Size of an individual SNOD entry
    private final int snodEntryStorageSize = snodEntrySize * 10;

    // **Dataset Storage (Where Raw Data Begins)**
    // The byte offset where actual dataset data is stored.
    // Everything before this is metadata.
    private final int dataAddress = snodAddress + snodSize + snodEntryStorageSize;

    public HdfFile(String fileName, StandardOpenOption[] openOptions) {
        this.fileName = fileName;
        this.openOptions = openOptions;

        // 100320
        superblock = new HdfSuperblock(0, 0, 0, 0,
                (short)8, (short)8,
                4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.of(firstGroupAddress),
                HdfFixedPoint.undefined((short)8));
        // Define a root group
        rootGroupEntry = new HdfSymbolTableEntry(HdfFixedPoint.of(0), HdfFixedPoint.of(objectHeaderPrefixAddress), 1,
                HdfFixedPoint.of(btreeAddress),
                HdfFixedPoint.of(localHeapAddress));

        objectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.of(btreeAddress),
                        HdfFixedPoint.of(localHeapAddress))));

        // Define the heap data size, why 88 I don't know.
        // Initialize the heapData array
        byte[] heapData = new byte[localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;

        localHeap = new HdfLocalHeap(HdfFixedPoint.of(localHeapContentsSize), HdfFixedPoint.of(localHeapContentsAddress));
        localHeapContents = new HdfLocalHeapContents(heapData);
        localHeap.addToHeap(new HdfString(new byte[0], false, false), this.localHeapContents);

        // Define a B-Tree for group indexing
        bTree = new HdfBTreeV1("TREE", 0, 0, 0,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8));

        groupSymbolTableNode = new HdfGroupSymbolTableNode("SNOD", 1, 0, new ArrayList<>());

        datasetRecordCount = new AtomicLong();

    }

    public HdfDataSet createDataSet(String datasetName, CompoundDataType compoundType) {
        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), false, false);
        // real steps needed to add a group.
        // entry in btree = "Demand" + snodOffset (1880)
        // entry in locaheapcontents = "Demand" = datasetName
        int linkNameOffset = bTree.addGroup(hdfDatasetName, HdfFixedPoint.of(firstGroupAddress), localHeap, localHeapContents);
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(HdfFixedPoint.of(linkNameOffset), HdfFixedPoint.of(firstGroupAddress), 0, HdfFixedPoint.undefined((short) 8), HdfFixedPoint.undefined((short) 8));
        groupSymbolTableNode.addEntry(ste);
        // entry in snod = linkNameOffset=8, objectHeaderAddress=800, cacheType=0,
        return new HdfDataSet(this, datasetName, compoundType, HdfFixedPoint.of(dataAddress));
    }

    public void write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
        datasetRecordCount.set(0);
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            long dataAddress = hdfDataSet.getDatasetAddress().getBigIntegerValue().longValue();
            fileChannel.position(dataAddress);
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                datasetRecordCount.incrementAndGet();
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
            }
        }
    }
//
//    public <T> void closeDataset(HdfDataSet<T> hdfDataSet) throws IOException {
//        long dataSize = hdfDataSet.updateForRecordCount(datasetRecordCount.get());
//        long endOfFile = dataAddress + dataSize;
//        superblock.setEndOfFileAddress(HdfFixedPoint.of(endOfFile));
//    }

    public void close() throws IOException {
        Path path = Path.of(fileName);
        StandardOpenOption[] fileOptions = {StandardOpenOption.WRITE};
        if ( !Files.exists(path) ) {
            fileOptions =new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE};
        }
        try (FileChannel fileChannel = FileChannel.open(path, fileOptions)) {
            // TODO: Implement actual serialization logic
            //        System.out.println("Superblock: " + superblock);
            // Allocate a buffer of size 2208
            // Get the data address directly from the single dataObject
//            HdfSuperblock superblock = hdfDataSet.getHdfFile().getSuperblock();
//            Optional<HdfFixedPoint> optionalDataAddress = dataObjectHeaderPrefix.getDataAddress();

//            // Extract the data start location dynamically
//            long dataStart = optionalDataAddress
//                    .map(HdfFixedPoint::getBigIntegerValue)
//                    .map(BigInteger::longValue)
//                    .orElseThrow(() -> new IllegalStateException("No Data Layout Message found"));
            int dataStart = 0;
            if ( bTree.getEntriesUsed() <= 0 ) {
                dataStart = superblock.getEndOfFileAddress().getBigIntegerValue().intValue();
            }

            // Allocate the buffer dynamically up to the data start location
            ByteBuffer buffer = ByteBuffer.allocate(dataStart).order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian

            //        System.out.println(superblock);
            // Write the superblock at position 0
            buffer.position(0);
            superblock.writeToByteBuffer(buffer);

            System.out.println(rootGroupEntry);
            // Write the root group symbol table entry immediately after the superblock
            rootGroupEntry.writeToByteBuffer(buffer, superblock.getSizeOfOffsets());

            System.out.println(objectHeaderPrefix);
            // Write Object Header at position found in rootGroupEntry
            int objectHeaderAddress = rootGroupEntry.getObjectHeaderAddress().getBigIntegerValue().intValue();
            buffer.position(objectHeaderAddress);
            objectHeaderPrefix.writeToByteBuffer(buffer);

            long localHeapPosition = -1;
            long bTreePosition = -1;

            // Try getting the Local Heap Address from the Root Symbol Table Entry
            if (rootGroupEntry.getLocalHeapAddress() != null && !rootGroupEntry.getLocalHeapAddress().isUndefined()) {
                localHeapPosition = rootGroupEntry.getLocalHeapAddress().getBigIntegerValue().longValue();
            }

            // If not found or invalid, fallback to Object Header's SymbolTableMessage
            Optional<SymbolTableMessage> symbolTableMessageOpt = objectHeaderPrefix.findHdfSymbolTableMessage(SymbolTableMessage.class);
            if (symbolTableMessageOpt.isPresent()) {
                SymbolTableMessage symbolTableMessage = symbolTableMessageOpt.get();

                // Retrieve Local Heap Address if still not found
                if (localHeapPosition == -1 && symbolTableMessage.getLocalHeapAddress() != null && !symbolTableMessage.getLocalHeapAddress().isUndefined()) {
                    localHeapPosition = symbolTableMessage.getLocalHeapAddress().getBigIntegerValue().longValue();
                }

                // Retrieve B-Tree Address
                if (symbolTableMessage.getBTreeAddress() != null && !symbolTableMessage.getBTreeAddress().isUndefined()) {
                    bTreePosition = symbolTableMessage.getBTreeAddress().getBigIntegerValue().longValue();
                }
            }

            // Validate B-Tree Position and write it
            if (bTreePosition != -1) {
                System.out.println(bTree);
                buffer.position((int) bTreePosition); // Move to the correct position
                bTree.writeToByteBuffer(buffer);
            } else {
                throw new IllegalStateException("No valid B-Tree position found.");
            }

            // Validate Local Heap Position and write it
            if (localHeapPosition != -1) {
                buffer.position((int) localHeapPosition); // Move to the correct position
                localHeap.writeToByteBuffer(buffer);
                buffer.position(localHeap.getDataSegmentAddress().getBigIntegerValue().intValue());
                localHeapContents.writeToByteBuffer(buffer);
            } else {
                throw new IllegalStateException("No valid Local Heap position found.");
            }
            buffer.flip();
            fileChannel.position(0);
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        }
    }
}
