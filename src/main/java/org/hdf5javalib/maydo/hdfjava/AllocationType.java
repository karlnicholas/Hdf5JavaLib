package org.hdf5javalib.maydo.hdfjava;

/**
 * Defines the types of allocation blocks used in an HDF5 file.
 * <p>
 * Each enum constant represents a specific type of storage block, with associated metadata
 * or data region assignments and typical offset and size characteristics.
 * </p>
 */
public enum AllocationType {
    /**
     * Superblock, located at offset 0 with a size of 96 bytes.
     */
    SUPERBLOCK,
    /**
     * Object header prefix for the root group, typically at offset 96 with a size of 40 bytes.
     */
    GROUP_OBJECT_HEADER,
    /**
     * B-tree node and storage, typically at offset 136 with a size of 544 bytes.
     */
    BTREE_HEADER,
    /**
     * Local heap header, typically at offset 680 with a size of 32 bytes.
     */
    LOCAL_HEAP_HEADER,
    /**
     * Dataset header, stored in the metadata region.
     */
    DATASET_OBJECT_HEADER,
    /**
     * Dataset continuation block, stored in the metadata region.
     */
    DATASET_HEADER_CONTINUATION,
    /**
     * Dataset data block, stored in the data region.
     */
    DATASET_DATA,
    /**
     * Active local heap contents, stored in the metadata region.
     */
    LOCAL_HEAP,
    /**
     * Abandoned local heap contents, stored in the metadata region.
     */
    LOCAL_HEAP_ABANDONED,
    /**
     * Symbol table node (SNOD), stored in the metadata region.
     */
    SNOD,
    /**
     * First global heap block, stored in the data region with a fixed size.
     */
    GLOBAL_HEAP_1,
    /**
     * Second global heap block, stored in the data region and expandable.
     */
    GLOBAL_HEAP_2
}
