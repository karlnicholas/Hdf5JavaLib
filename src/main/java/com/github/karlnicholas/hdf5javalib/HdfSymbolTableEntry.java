package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.utils.HdfUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class HdfSymbolTableEntry {
    private final HdfFixedPoint linkNameOffset;
    private final HdfFixedPoint objectHeaderAddress;
    private final int cacheType;
    private final HdfFixedPoint bTreeAddress;
    private final HdfFixedPoint localHeapAddress;

    public HdfSymbolTableEntry(
            HdfFixedPoint linkNameOffset,
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

    public static HdfSymbolTableEntry fromFileChannel(FileChannel fileChannel, int offsetSize) throws IOException {
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

        return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, cacheType, bTreeAddress, localHeapAddress);
    }

    public static HdfSymbolTableEntry fromByteBuffer(ByteBuffer buffer, int offsetSize) {
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        HdfFixedPoint objectHeaderAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);

        // Read cache type and skip reserved field
        int cacheType = buffer.getInt();
        buffer.position(buffer.position() + 4);

        // Initialize addresses for cacheType 1
        HdfFixedPoint bTreeAddress = null;
        HdfFixedPoint localHeapAddress = null;
        if (cacheType == 1) {
            bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
            localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);
        } else {
            buffer.position(buffer.position() + 16);
        }

        return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, cacheType, bTreeAddress, localHeapAddress);
    }

    public HdfFixedPoint getLinkNameOffset() {
        return linkNameOffset;
    }

    public HdfFixedPoint getObjectHeaderAddress() {
        return objectHeaderAddress;
    }

    public int getCacheType() {
        return cacheType;
    }

    public HdfFixedPoint getBTreeAddress() {
        return bTreeAddress;
    }

    public HdfFixedPoint getLocalHeapAddress() {
        return localHeapAddress;
    }

    @Override
    public String toString() {
        return "HdfSymbolTableEntry{" +
                "linkNameOffset=" + linkNameOffset.getBigIntegerValue() +
                ", objectHeaderAddress=" + objectHeaderAddress.getBigIntegerValue() +
                ", cacheType=" + cacheType +
                ", bTreeAddress=" + (bTreeAddress != null ? bTreeAddress.getBigIntegerValue() : "N/A") +
                ", localHeapAddress=" + (localHeapAddress != null ? localHeapAddress.getBigIntegerValue() : "N/A") +
                '}';
    }
}
