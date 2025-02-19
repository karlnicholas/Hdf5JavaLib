package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.utils.HdfUtils;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
public class HdfSymbolTableEntry {
    private final HdfFixedPoint linkNameOffset;
    private final HdfObjectHeaderPrefixV1 objectHeader;
    private final int cacheType;
    private final HdfBTreeV1 bTree;
    private final HdfLocalHeap localHeap;

    // cache type 1
    public HdfSymbolTableEntry(
            HdfFixedPoint linkNameOffset,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap
    ) {
        this.cacheType = 1;
        this.linkNameOffset = linkNameOffset;
        this.objectHeader = objectHeader;
        this.bTree = bTree;
        this.localHeap = localHeap;
    }

    @Getter
    public static class HdfFileOffsets {
        private final HdfFixedPoint linkNameOffset;
        private final HdfFixedPoint objectHeaderAddress;
        private final int cacheType;
        private final HdfFixedPoint bTreeAddress;
        private final HdfFixedPoint localHeapAddress;
        public HdfFileOffsets(HdfFixedPoint linkNameOffset,
                              HdfFixedPoint objectHeaderAddress,
                              int cacheType,
                              HdfFixedPoint bTreeAddress,
                              HdfFixedPoint localHeapAddress
        ) {
            this.linkNameOffset = linkNameOffset;
            this.objectHeaderAddress = objectHeaderAddress;
            this.cacheType = cacheType;
            this.bTreeAddress = bTreeAddress;
            this.localHeapAddress = localHeapAddress;
        }
    }

    public static HdfFileOffsets fromFileChannel(FileChannel fileChannel, short offsetSize) throws IOException {
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);
        HdfFixedPoint objectHeaderAddress = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);

        // Read cache type and skip reserved field
        int cacheType = HdfUtils.readIntFromFileChannel(fileChannel);
        HdfUtils.skipBytes(fileChannel, 4); // Skip reserved field

        // Initialize addresses for cacheType 1
        HdfFixedPoint bTreeAddress = null;
        HdfFixedPoint localHeapAddress = null;
        if (cacheType == 1) {
            bTreeAddress = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);
            localHeapAddress = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);
        } else {
            HdfUtils.skipBytes(fileChannel, 16); // Skip 16 bytes for scratch-pad
        }

        return new HdfFileOffsets(linkNameOffset, objectHeaderAddress, cacheType, bTreeAddress, localHeapAddress);
    }

//    public static HdfSymbolTableEntry fromByteBuffer(ByteBuffer buffer, short offsetSize) {
//        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
//        HdfFixedPoint linkNameOffset = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
//        HdfFixedPoint objectHeaderAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
//
//        // Read cache type and skip reserved field
//        int cacheType = buffer.getInt();
//        buffer.position(buffer.position() + 4);
//
//        // Initialize addresses for cacheType 1
//        HdfFixedPoint bTreeAddress = null;
//        HdfFixedPoint localHeapAddress = null;
//        if (cacheType == 1) {
//            bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
//            localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
//        } else {
//            buffer.position(buffer.position() + 16);
//        }
//
//        return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, cacheType, bTreeAddress, localHeapAddress);
//    }

//    public void writeToByteBuffer(ByteBuffer buffer, int offsetSize) {
//        // Write Link Name Offset (sizeOfOffsets bytes, little-endian)
//        writeFixedPointToBuffer(buffer, linkNameOffset);
//
//        // Write Object Header Address (sizeOfOffsets bytes, little-endian)
//        writeFixedPointToBuffer(buffer, objectHeaderAddress);
//
//        // Write Cache Type (4 bytes, little-endian)
//        buffer.putInt(cacheType);
//
//        // Write Reserved Field (4 bytes, must be 0)
//        buffer.putInt(0);
//
//        // If cacheType == 1, write B-tree Address and Local Heap Address
//        if (cacheType == 1) {
//            writeFixedPointToBuffer(buffer, bTreeAddress);
//            writeFixedPointToBuffer(buffer, localHeapAddress);
//        } else {
//            // If cacheType != 1, write 16 bytes of reserved "scratch-pad" space
//            buffer.put(new byte[16]);
//        }
//    }

    @Override
    public String toString() {
        return "HdfSymbolTableEntry{" +
                "linkNameOffset=" + (linkNameOffset != null ? linkNameOffset.toString() : "N/A") +
                ", objectHeader=" + (objectHeader != null ? objectHeader.toString() : "N/A") +
                ", cacheType=" + cacheType +
                ", bTree=" + (bTree != null ? bTree.toString() : "N/A") +
                ", localHeap=" + (localHeap != null ? localHeap.toString() : "N/A") +
                "}";
    }
}
