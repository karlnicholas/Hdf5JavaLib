package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
public class HdfLocalHeap {
    private final String signature;
    private final int version;
    private final HdfFixedPoint dataSegmentSize;
    private HdfFixedPoint freeListOffset;
    private final HdfFixedPoint dataSegmentAddress;

    public HdfLocalHeap(String signature, int version, HdfFixedPoint dataSegmentSize, HdfFixedPoint freeListOffset, HdfFixedPoint dataSegmentAddress) {
        this.signature = signature;
        this.version = version;
        this.dataSegmentSize = dataSegmentSize;
        this.freeListOffset = freeListOffset;
        this.dataSegmentAddress = dataSegmentAddress;
    }

    public HdfLocalHeap(String heap, int version, HdfFixedPoint dataSegmentSize, HdfFixedPoint dataSegmentAddress) {
        this("HEAP", 1, dataSegmentSize, HdfFixedPoint.of(0), dataSegmentAddress);
    }

    public void addToHeap(HdfString objectName, HdfLocalHeapContents localHeapContents) {
        byte[] objectNameBytes = objectName.getHdfBytes();
        int freeListOffset = this.freeListOffset.getBigIntegerValue().intValue();
        System.arraycopy(objectNameBytes, 0, localHeapContents.getHeapData(), freeListOffset, objectNameBytes.length);
        freeListOffset = (freeListOffset + 7) & ~7;
        this.freeListOffset = HdfFixedPoint.of(freeListOffset);
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

    public void nullStringAtPositionZero(HdfLocalHeapContents localHeapContents) {
        System.arraycopy(new byte[8], 0, localHeapContents.getHeapData(), 0, 8);
        freeListOffset = HdfFixedPoint.of(8);
    }
}
