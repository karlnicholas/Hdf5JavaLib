package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.dataobjects.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.datatype.*;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.*;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.utils.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.*;

public class HdfFileBuilder {
    private HdfSuperblock superblock;
    private HdfSymbolTableEntry rootGroupEntry;
    private HdfObjectHeaderPrefixV1 objectHeaderPrefix;
    private HdfLocalHeap localHeap;
    private HdfLocalHeapContents localHeapContents;
    private HdfBTreeV1 bTree;
    private HdfSymbolTableNode symbolTableNode;
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;



    /**
     * Basic Allocation Type	Description
     * H5FD_MEM_SUPER	File space allocated for Superblock.
     * H5FD_MEM_BTREE	File space allocated for B-tree.
     * H5FD_MEM_DRAW	File space allocated for raw data.
     * H5FD_MEM_GHEAP	File space allocated for Global Heap.
     * H5FD_MEM_LHEAP	File space allocated for Local Heap.
     * H5FD_MEM_OHDR	File space allocated for Object Header.
     *
     */


    public void superblock(
            int groupLeafNodeK,
            int groupInternalNodeK,
            long baseAddress,
            long endOfFileAddress
    ) {
        this.superblock = new HdfSuperblock(0, 0, 0, 0, (short)8, (short)8, groupLeafNodeK, groupInternalNodeK,
                HdfFixedPoint.of(baseAddress),
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.of(endOfFileAddress),
                HdfFixedPoint.undefined((short)8));
    }

    public void localHeap(long dataSegmentSize, long freeListOffset, long dataSegmentAddress, byte[] data) {
        this.localHeap = new HdfLocalHeap("HEAP", 0,
                HdfFixedPoint.of(dataSegmentSize),
                HdfFixedPoint.of(freeListOffset),
                HdfFixedPoint.of(dataSegmentAddress));
        this.localHeapContents = new HdfLocalHeapContents(data);
    }

    /** Adds a group to the HDF5 file */
    public HdfFileBuilder rootGroup(long objectHeaderAddress) {
        HdfFixedPoint objHeaderAddr = HdfFixedPoint.of(objectHeaderAddress);

        rootGroupEntry = new HdfSymbolTableEntry(HdfFixedPoint.of(0), objHeaderAddr, 1,
                HdfFixedPoint.of(136),
                HdfFixedPoint.of(680));
        return this;
    }

    /** Adds a group to the HDF5 file */
    public HdfFileBuilder objectHeader() {
        objectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.of(136),
                        HdfFixedPoint.of(680))));

        return this;
    }

    /** Adds a dataset to the HDF5 file */
    public HdfFileBuilder addDataset(List<HdfMessage> headerMessages) {
        int totalHeaderMessages = headerMessages.size();
        int objectReferenceCount = 1;
        int objectHeaderSize = 0;
        // 8, 1, 1064
        for( HdfMessage headerMessage: headerMessages ) {
            objectHeaderSize += headerMessage.getSizeMessageData();
        }
        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, totalHeaderMessages, objectReferenceCount, objectHeaderSize, headerMessages);

        return this;
    }

    /** Adds a B-Tree for Group Nodes */
    public HdfFileBuilder addBTree(long address, String objectName) {
        List<HdfFixedPoint> childPointers = Collections.singletonList(HdfFixedPoint.of(address));
        List<HdfFixedPoint> keys = Arrays.asList(
                HdfFixedPoint.of(0),
                HdfFixedPoint.of(8));

        List<BtreeV1GroupNode> groupNodes = Collections.singletonList(
                new BtreeV1GroupNode(new HdfString(objectName, false), HdfFixedPoint.of(address)));

        this.bTree = new HdfBTreeV1("TREE", 0, 0, 1,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8),
                childPointers, keys, groupNodes);
        return this;
    }

    /** Adds a symbol table node */
    public HdfFileBuilder addSymbolTableNode(long objectHeaderAddress) {
        List<HdfSymbolTableEntry> entries = Collections.singletonList(
                new HdfSymbolTableEntry(HdfFixedPoint.of(8), HdfFixedPoint.of(objectHeaderAddress),
                        0,null, null));
        this.symbolTableNode = new HdfSymbolTableNode("SNOD", 1, 1, entries);
        return this;
    }

    /** Writes the HDF5 file */
    public void writeToFile(FileChannel fileChannel) throws IOException {
        // TODO: Implement actual serialization logic
        System.out.println("Superblock: " + superblock);
        // Allocate a buffer of size 2208
        // Get the data address directly from the single dataObject
        Optional<HdfFixedPoint> optionalDataAddress = dataObjectHeaderPrefix.getDataAddress();

        // Extract the data start location dynamically
        long dataStart = optionalDataAddress
                .map(HdfFixedPoint::getBigIntegerValue)
                .map(BigInteger::longValue)
                .orElseThrow(() -> new IllegalStateException("No Data Layout Message found"));

        // Allocate the buffer dynamically up to the data start location
        ByteBuffer buffer = ByteBuffer.allocate((int) dataStart);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian

        System.out.println(superblock);
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

        int objectDataHeaderAddress = symbolTableNode.getSymbolTableEntries().get(0).getObjectHeaderAddress().getBigIntegerValue().intValue();
        buffer.position(objectDataHeaderAddress);
        System.out.println(dataObjectHeaderPrefix);
        dataObjectHeaderPrefix.writeToByteBuffer(buffer);
        buffer.flip();
        fileChannel.write(buffer);
        dumpByteBuffer(buffer);
    }

    public static void dumpByteBuffer(ByteBuffer buffer) {
        int bytesPerLine = 16; // 16 bytes per row
        int limit = buffer.limit();
        buffer.rewind(); // Reset position to 0 before reading

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < limit; i += bytesPerLine) {
            // Print the address (memory offset in hex)
            sb.append(String.format("%08X:  ", i));

            StringBuilder ascii = new StringBuilder();

            // Print the first 8 bytes (hex values)
            for (int j = 0; j < 8; j++) {
                buildHexValues(buffer, limit, sb, i, ascii, j);
            }

            sb.append(" "); // Space separator

            // Print the second 8 bytes (hex values)
            for (int j = 8; j < bytesPerLine; j++) {
                buildHexValues(buffer, limit, sb, i, ascii, j);
            }

            // Append ASCII representation
            sb.append("  ").append(ascii);

            // Newline for next row
            sb.append("\n");
        }

        System.out.print(sb);
    }

    private static void buildHexValues(ByteBuffer buffer, int limit, StringBuilder sb, int i, StringBuilder ascii, int j) {
        if (i + j < limit) {
            byte b = buffer.get(i + j);
            sb.append(String.format("%02X ", b));
            ascii.append(isPrintable(b) ? (char) b : '.');
        } else {
            sb.append("   "); // Padding for incomplete lines
            ascii.append(" ");
        }
    }

    // Helper method to check if a byte is a printable ASCII character (excluding control chars)
    private static boolean isPrintable(byte b) {
        return (b >= 32 && b <= 126); // Includes extended ASCII
    }

    public long dataAddress() {
        return dataObjectHeaderPrefix
                .findHdfSymbolTableMessage(DataLayoutMessage.class)
                .orElseThrow()
                .getDataAddress()
                .getBigIntegerValue()
                .longValue();
    }
}
