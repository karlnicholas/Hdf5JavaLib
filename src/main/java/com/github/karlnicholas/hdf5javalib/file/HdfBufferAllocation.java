package com.github.karlnicholas.hdf5javalib.file;

import lombok.Getter;
import lombok.Setter;

@Getter
public class HdfBufferAllocation {
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
    private final int dataGroupSize = 16;
    @Setter
    private int dataGroupStorageSize = 256;  // Total size of the first group's metadata
//    private int dataGroupStorageSize = 16 + 1064;  // Total size of the first group's metadata

    // **Symbol Table Node (SNOD)**
    // The SNOD (Symbol Table Node) organizes entries for objects within the group.
    // It manages links to datasets, other groups, and named datatypes.
    private final int snodAddress = dataGroupAddress + dataGroupSize + dataGroupStorageSize;
    private final int snodSize = 8;  // Header or control structure for the SNOD


    // **SNOD Entry (Represents Objects in the Group)**
    // An SNOD entry describes individual datasets or subgroups within the group.
    // Each entry in the SNOD table includes the object name, address, and type.
    private final int snodEntrySize = 32;  // Size of an individual SNOD entry
    private final int snodEntryStorageSize = snodEntrySize * 10;

    // **Dataset Storage (Where Raw Data Begins)**
    // The byte offset where actual dataset data is stored.
    // Everything before this is metadata.
    @Setter
    private int dataAddress = 2048;


}
