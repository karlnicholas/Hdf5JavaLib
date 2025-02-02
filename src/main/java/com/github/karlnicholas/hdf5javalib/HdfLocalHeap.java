package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class HdfLocalHeap {
    private final String signature;
    private final int version;
    private final HdfFixedPoint dataSegmentSize;
    private final HdfFixedPoint freeListOffset;
    private final HdfFixedPoint dataSegmentAddress;

    public HdfLocalHeap(String signature, int version, HdfFixedPoint dataSegmentSize, HdfFixedPoint freeListOffset, HdfFixedPoint dataSegmentAddress) {
        this.signature = signature;
        this.version = version;
        this.dataSegmentSize = dataSegmentSize;
        this.freeListOffset = freeListOffset;
        this.dataSegmentAddress = dataSegmentAddress;
    }

    public static HdfLocalHeap readFromFileChannel(FileChannel fileChannel, long position, short offsetSize, short lengthSize) throws IOException {
        // Allocate buffer for the local heap header
        ByteBuffer buffer = ByteBuffer.allocate(32); // Initial size for header parsing
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

        // Read data from file channel starting at the specified position
        fileChannel.position(position);
        fileChannel.read(buffer);
        buffer.flip();

        // Parse the signature
        byte[] signatureBytes = new byte[4];
        buffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"HEAP".equals(signature)) {
            throw new IllegalArgumentException("Invalid heap signature: " + signature);
        }

        // Parse the version
        int version = Byte.toUnsignedInt(buffer.get());

        // Parse reserved bytes
        byte[] reserved = new byte[3];
        buffer.get(reserved);
        if (!allBytesZero(reserved)) {
            throw new IllegalArgumentException("Reserved bytes in heap header must be zero.");
        }

        // Parse fixed-point fields using HdfFixedPoint
        HdfFixedPoint dataSegmentSize = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
        HdfFixedPoint freeListOffset = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, false);
        HdfFixedPoint dataSegmentAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, false);

        return new HdfLocalHeap(signature, version, dataSegmentSize, freeListOffset, dataSegmentAddress);
    }

    private static boolean allBytesZero(byte[] bytes) {
        for (byte b : bytes) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "HdfLocalHeap{" +
                "signature='" + signature + '\'' +
                ", version=" + version +
                ", dataSegmentSize=" + dataSegmentSize +
                ", freeListOffset=" + freeListOffset +
                ", dataSegmentAddress=" + dataSegmentAddress +
                '}';
    }

    public String getSignature() {
        return signature;
    }

    public int getVersion() {
        return version;
    }

    public HdfFixedPoint getDataSegmentSize() {
        return dataSegmentSize;
    }

    public HdfFixedPoint getFreeListOffset() {
        return freeListOffset;
    }

    public HdfFixedPoint getDataSegmentAddress() {
        return dataSegmentAddress;
    }
}
