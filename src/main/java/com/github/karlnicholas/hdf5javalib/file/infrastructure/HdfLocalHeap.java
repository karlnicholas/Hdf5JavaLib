package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
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

    public static HdfLocalHeap readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize) throws IOException {
        // Allocate buffer for the local heap header
        ByteBuffer buffer = ByteBuffer.allocate(32); // Initial size for header parsing
        buffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

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

    public void writeToByteBuffer(ByteBuffer buffer) {
        // Step 1: Write the "HEAP" signature (4 bytes)
        buffer.put(signature.getBytes());

        // Step 2: Write the version (1 byte)
        buffer.put((byte) version);

        // Step 3: Write reserved bytes (3 bytes, must be 0)
        buffer.put(new byte[3]);

        // Step 4: Write Data Segment Size (lengthSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, dataSegmentSize);

        // Step 5: Write Free List Offset (lengthSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, freeListOffset);

        // Step 6: Write Data Segment Address (offsetSize bytes, little-endian)
        writeFixedPointToBuffer(buffer, dataSegmentAddress);
    }

}
