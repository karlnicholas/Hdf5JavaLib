package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.utils.HdfParseUtils;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

@Getter
public class HdfSymbolTableEntry {
    private final HdfFixedPoint linkNameOffset;
    private final HdfFixedPoint objectHeaderAddress;
    private final int cacheType;
    private final HdfFixedPoint bTreeAddress;
    private final HdfFixedPoint localHeapAddress;

    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderAddress, HdfFixedPoint bTreeAddress, HdfFixedPoint localHeapAddress) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderAddress = objectHeaderAddress;
        this.cacheType = 1;
        this.bTreeAddress = bTreeAddress;
        this.localHeapAddress = localHeapAddress;
    }

    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderAddress) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderAddress = objectHeaderAddress;
        this.cacheType = 0;
        this.bTreeAddress = null;
        this.localHeapAddress = null;
    }

    public static HdfSymbolTableEntry fromFileChannel(FileChannel fileChannel, short offsetSize) throws IOException {
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);
        HdfFixedPoint objectHeaderAddress = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);

        // Read cache type and skip reserved field
        int cacheType = HdfParseUtils.readIntFromFileChannel(fileChannel);
        HdfParseUtils.skipBytes(fileChannel, 4); // Skip reserved field

        // Initialize addresses for cacheType 1
        if (cacheType == 1) {
            HdfFixedPoint bTreeAddress = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);
            HdfFixedPoint localHeapAddress = HdfFixedPoint.readFromFileChannel(fileChannel, offsetSize, false);
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, bTreeAddress, localHeapAddress);
        } else {
            HdfParseUtils.skipBytes(fileChannel, 16); // Skip 16 bytes for scratch-pad
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress);
        }
    }

    public static HdfSymbolTableEntry fromByteBuffer(ByteBuffer buffer, short offsetSize) {
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        HdfFixedPoint objectHeaderAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);

        // Read cache type and skip reserved field
        int cacheType = buffer.getInt();
        buffer.position(buffer.position() + 4);

        // Initialize addresses for cacheType 1
        if (cacheType == 1) {
            HdfFixedPoint bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
            HdfFixedPoint localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, bTreeAddress, localHeapAddress);
        } else {
            buffer.position(buffer.position() + 16);
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress);
        }

    }

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
        StringBuilder sb = new StringBuilder("HdfSymbolTableEntry{");
        sb.append("linkNameOffset=").append(linkNameOffset)
                .append(", objectHeaderAddress=").append(objectHeaderAddress)
                .append(", cacheType=").append(cacheType);

        switch (cacheType) {
            case 0:
                break; // Base fields only
            case 1:
                sb.append(", bTreeAddress=").append(bTreeAddress)
                        .append(", localHeapAddress=").append(localHeapAddress);
                break;
            default:
                throw new IllegalStateException("Unknown cache type: " + cacheType);
        }

        sb.append("}");
        return sb.toString();
    }
}
