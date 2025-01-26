package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.message.HdfMessage;
import com.github.karlnicholas.hdf5javalib.message.SymbolTableMessage;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

public class HdfConstruction {
    // level 0
    private HdfSuperblock superblock;
    private HdfSymbolTableEntry rootGroupSymbolTableEntry;
    private HdfObjectHeaderV1 objectHeader;
    // level 1A
    private HdfBTreeV1 bTree;
    // level 1D
    private HdfLocalHeap localHeap;
    private HdfLocalHeapContents localHeapContents;
    // level 2A1
    private HdfDataObjectHeaderPrefixV1 dataObjectHeaderPrefix;
    // llevel 2AA
    private List<HdfMessage> dataObjectHeaderMessages;

    public void buildHfd() {
        buildSuperblock();
        buildRooTSymbolTableEntry();
    }

    private void buildSuperblock() {
        // Create "undefined" HdfFixedPoint instances
        HdfFixedPoint undefinedAddress = HdfFixedPoint.undefined(8); // Size of offsets = 8

        // Create specific HdfFixedPoint instances for defined values
        HdfFixedPoint baseAddress = new HdfFixedPoint(BigInteger.ZERO, true); // Base address = 0, little-endian
        HdfFixedPoint endOfFileAddress = new HdfFixedPoint(BigInteger.valueOf(100320), true); // EOF address = 100320, little-endian

        // Construct the HdfSuperblock instance
        HdfSuperblock superblock = new HdfSuperblock(
                0,                          // version
                0,                          // freeSpaceVersion
                0,                          // rootGroupVersion
                0,                          // sharedHeaderVersion
                8,                          // sizeOfOffsets
                8,                          // sizeOfLengths
                4,                          // groupLeafNodeK
                16,                         // groupInternalNodeK
                baseAddress,                // baseAddress
                undefinedAddress,           // freeSpaceAddress
                endOfFileAddress,           // endOfFileAddress
                undefinedAddress            // driverInformationAddress
        );
    }

    private void buildRooTSymbolTableEntry() {
        // Create specific HdfFixedPoint instances for the required addresses
        HdfFixedPoint linkNameOffset = new HdfFixedPoint(BigInteger.ZERO, true); // Link name offset = 0
        HdfFixedPoint objectHeaderAddress = new HdfFixedPoint(BigInteger.valueOf(96), true); // Object header address = 96
        HdfFixedPoint bTreeAddress = new HdfFixedPoint(BigInteger.valueOf(136), true); // B-tree address = 136
        HdfFixedPoint localHeapAddress = new HdfFixedPoint(BigInteger.valueOf(680), true); // Local heap address = 680

        // Construct the HdfSymbolTableEntry instance
        rootGroupSymbolTableEntry = new HdfSymbolTableEntry(
                linkNameOffset,
                objectHeaderAddress,
                1,                // Cache type = 1
                bTreeAddress,
                localHeapAddress
        );
    }

    private void buildObjectHeader() {
        // Create HdfFixedPoint instances for the SymbolTableMessage
        HdfFixedPoint bTreeAddress = new HdfFixedPoint(BigInteger.valueOf(136), true); // B-tree address = 136
        HdfFixedPoint localHeapAddress = new HdfFixedPoint(BigInteger.valueOf(680), true); // Local heap address = 680

        // Create a SymbolTableMessage directly
        SymbolTableMessage symbolTableMessage = new SymbolTableMessage(bTreeAddress, localHeapAddress);

        // Create the HdfObjectHeaderV1 instance
        HdfObjectHeaderV1 objectHeader = new HdfObjectHeaderV1(
                1, // Version
                1, // Total header messages
                1, // Object reference count
                24, // Object header size
                Collections.singletonList(symbolTableMessage) // List of header messages
        );

    }
}
