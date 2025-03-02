package org.hdf5javalib.file;

import lombok.Getter;

@Getter
public class HdfBufferAllocation {
    public HdfBufferAllocation() {
        recalc();
    }
    private void recalc() {
        objectHeaderPrefixAddress = superblockAddress + superblockSize;
        btreeAddress = objectHeaderPrefixAddress + objectHeaderPrefixSize;
        localHeapAddress = btreeAddress + btreeSize + btreeStorageSize;
        localHeapContentsAddress = localHeapAddress + localHeapSize;
        dataGroupAddress = localHeapContentsAddress + localHeapContentsSize;
        snodAddress = dataGroupAddress + dataGroupSize + dataGroupStorageSize;
        int lastAddressUsed = snodAddress + snodSize + snodEntryStorageSize;
        messageContinuationAddress = lastAddressUsed;
        dataAddress = Math.max(2048, lastAddressUsed + messageContinuationSpace);
//        if ( lastAddressUsed > metaDataSize) {
//            throw new IllegalStateException("lastAddressUsed > dataAddress, " + lastAddressUsed + dataAddress);
//        }
    }

    public void setDataGroupAndCOntinuationStorageSize(int objectHeaderSize, int continueSize) {
        this.dataGroupStorageSize = objectHeaderSize;
        this.messageContinuationSpace = continueSize;
        recalc();
    }
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
    private final int superblockSize = 96;    // Superblock size (metadata about the file)

    // **Object Header Prefix (Metadata about the First Group)**
    // This section contains metadata for the first group, defining attributes,
    // storage layout, and dataset properties.
    private int objectHeaderPrefixAddress;
    private final int objectHeaderPrefixSize = 40;

    // **B-tree (Manages Group Links)**
    // The B-tree is used to efficiently organize links within the group,
    // allowing quick access to datasets and subgroups.
    private int btreeAddress;
    private final int btreeSize = 32;  // Size of a B-tree node
    private final int btreeStorageSize = 512;  // Allocated storage for B-tree nodes

    // **Local Heap (Stores Group Names & Small Objects)**
    // The local heap stores small metadata elements such as object names
    // and soft links, reducing fragmentation in the file.
    private int localHeapAddress;
    private final int localHeapSize = 32;  // Header for the local heap
    private int localHeapContentsAddress;  // Contents stored inside the heap
    private final int localHeapContentsSize = 88;  // Contents stored inside the heap

    // **First Group Address (Computed from Previous Structures)**
    // This is the byte offset where the first group starts in the file.
    // It is calculated based on the sum of all preceding metadata structures.
    private int dataGroupAddress;
    private final int dataGroupSize = 16;
    private int dataGroupStorageSize = 256;  // Total size of the first group's metadata

    // **Symbol Table Node (SNOD)**
    // The SNOD (Symbol Table Node) organizes entries for objects within the group.
    // It manages links to datasets, other groups, and named datatypes.
    private int snodAddress;
    private final int snodSize = 8;  // Header or control structure for the SNOD


    // **SNOD Entry (Represents Objects in the Group)**
    // An SNOD entry describes individual datasets or subgroups within the group.
    // Each entry in the SNOD table includes the object name, address, and type.
    private final int snodEntrySize = 32;  // Size of an individual SNOD entry
    private final int snodEntryStorageSize = snodEntrySize * 10;

    private int messageContinuationSpace = 0;
    private int messageContinuationAddress;

    // **Dataset Storage (Where Raw Data Begins)**
    // The byte offset where actual dataset data is stored.
    // Everything before this is metadata.
    private int dataAddress;

}
