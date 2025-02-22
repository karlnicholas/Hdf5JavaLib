package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfDatatype;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.DataspaceMessage;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

@Getter
public class HdfFile {
    private final String fileName;
    private final StandardOpenOption[] openOptions;
    // initial setup without Dataset
    private final HdfSuperblock superblock;
    private final HdfSymbolTableEntry rootSymbolTableEntry;
    private final HdfGroup rootGroup;

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
    private final int rootSymbolTableEntryAddress = superblockAddress + superblockSize;
    private final int rootSymbolTableEntrySize = 40;

    // **Object Header Prefix (Metadata about the First Group)**
    // This section contains metadata for the first group, defining attributes,
    // storage layout, and dataset properties.
    private final int objectHeaderPrefixAddress = rootSymbolTableEntryAddress + rootSymbolTableEntrySize;
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
    private final int dataGroupAddress = localHeapContentsAddress + localHeapContentsSize;
    private final int dataGroupStorageSize = 256;  // Total size of the first group's metadata
//    private int dataGroupStorageSize = 16 + 1064;  // Total size of the first group's metadata

    // **Symbol Table Node (SNOD)**
    // The SNOD (Symbol Table Node) organizes entries for objects within the group.
    // It manages links to datasets, other groups, and named datatypes.
    private final int snodAddress = dataGroupAddress + dataGroupStorageSize;
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
                HdfFixedPoint.of(dataAddress),
                HdfFixedPoint.undefined((short)8));
        rootSymbolTableEntry = new HdfSymbolTableEntry(
                HdfFixedPoint.of(0),
                HdfFixedPoint.of(objectHeaderPrefixAddress),
                HdfFixedPoint.of(btreeAddress),
                HdfFixedPoint.of(localHeapAddress));

        rootGroup = new HdfGroup(this, "", btreeAddress, localHeapAddress);
    }

    /**
     * by default, the root group.
     * @param datasetName String
     * @param hdfDatatype HdfDatatype
     * @param dataSpaceMessage DataspaceMessage
     * @return HdfDataSet
     */
    public HdfDataSet createDataSet(String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        return rootGroup.createDataSet(datasetName, hdfDatatype, dataSpaceMessage, dataGroupAddress);
    }

    public long write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            fileChannel.position(getDataAddress());
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
            }
        }
        return getDataAddress();
    }
//
//    public <T> void closeDataset(HdfDataSet<T> hdfDataSet) throws IOException {
//        long dataSize = hdfDataSet.updateForRecordCount(datasetRecordCount.get());
//        long endOfFile = dataAddress + dataSize;
//        superblock.setEndOfFileAddress(HdfFixedPoint.of(endOfFile));
//    }

    public void close() throws IOException {
//        int dataStart = 0;
//        hdfGroup.writeToBuffer();
//        if ( bTree.getEntriesUsed() <= 0 ) {
//            dataStart = superblock.getEndOfFileAddress().getBigIntegerValue().intValue();
//        }

        System.out.println(superblock);
        System.out.println(rootSymbolTableEntry);
        System.out.println(rootGroup);

        // Allocate the buffer dynamically up to the data start location
        ByteBuffer buffer = ByteBuffer.allocate(dataAddress).order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian
        buffer.position(superblockAddress);
        superblock.writeToByteBuffer(buffer);
        buffer.position(rootSymbolTableEntryAddress);
        rootSymbolTableEntry.writeToBuffer(buffer);
        buffer.position(objectHeaderPrefixAddress);
        rootGroup.writeToBuffer(buffer);

        buffer.flip();
//        rootGroup.close(buffer);
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
            fileChannel.position(0);
            //        System.out.println(superblock);
            // Write the superblock at position 0
            buffer.position(0);

            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }

        }
    }
}
