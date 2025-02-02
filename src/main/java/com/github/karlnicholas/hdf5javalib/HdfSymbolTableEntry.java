package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.utils.HdfUtils;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

@Getter
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

    public static HdfSymbolTableEntry fromFileChannel(FileChannel fileChannel, short offsetSize) throws IOException {
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

    public static HdfSymbolTableEntry fromByteBuffer(ByteBuffer buffer, short offsetSize) {
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

    public void writeToByteBuffer(ByteBuffer buffer, int offsetSize) {
        // Write Link Name Offset (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, linkNameOffset, offsetSize);

        // Write Object Header Address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, objectHeaderAddress, offsetSize);

        // Write Cache Type (4 bytes, little-endian)
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        // If cacheType == 1, write B-tree Address and Local Heap Address
        if (cacheType == 1) {
            writeFixedPointToBuffer(buffer, bTreeAddress, offsetSize);
            writeFixedPointToBuffer(buffer, localHeapAddress, offsetSize);
        } else {
            // If cacheType != 1, write 16 bytes of reserved "scratch-pad" space
            buffer.put(new byte[16]);
        }
    }

    /**
     * Writes an `HdfFixedPoint` value to the `ByteBuffer` in **little-endian format**.
     * If the value is undefined, it writes `0xFF` for all bytes.
     */
    private void writeFixedPointToBuffer(ByteBuffer buffer, HdfFixedPoint value, int size) {
        byte[] bytesToWrite = new byte[size];

        if (value.isUndefined()) {
            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
        } else {
            byte[] valueBytes = value.getBigIntegerValue().toByteArray();
            int copySize = Math.min(valueBytes.length, size);

            // Store in **little-endian format** by reversing byte order
            for (int i = 0; i < copySize; i++) {
                bytesToWrite[i] = valueBytes[copySize - 1 - i];
            }
        }

        buffer.put(bytesToWrite);
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
