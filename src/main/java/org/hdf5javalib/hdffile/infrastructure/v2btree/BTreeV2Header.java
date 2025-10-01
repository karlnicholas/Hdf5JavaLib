package org.hdf5javalib.hdffile.infrastructure.v2btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

// --- Header ---
public class BTreeV2Header {
    public final String signature;
    public final short version;
    public final BTreeV2Type type;
    public final int nodeSize;
    public final int recordSize;
    public final int depth;
    public final short splitPercent;
    public final short mergePercent;
    public final long rootNodeAddress;
    public final int numberOfRecordsInRootNode;
    public final long totalNumberOfRecordsInBTree;
    public final int checksum;

    private BTreeV2Header(ByteBuffer bb, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        Hdf5Utils.V2_verifySignature(bb, "BTHD");
        this.signature = "BTHD";
        this.version = bb.get();
        this.type = BTreeV2Type.from(bb.get());
        this.nodeSize = bb.getInt();
        this.recordSize = bb.getShort() & 0xFFFF;
        this.depth = bb.getShort() & 0xFFFF;
        this.splitPercent = bb.get();
        this.mergePercent = bb.get();
        this.rootNodeAddress = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        this.numberOfRecordsInRootNode = bb.getShort() & 0xFFFF;
        this.totalNumberOfRecordsInBTree = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.checksum = Hdf5Utils.readChecksum(bb);
    }

    public static BTreeV2Header read(SeekableByteChannel channel, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        int headerSize = 4 // Signature
                + 1 // Version
                + 1 // Type
                + 4 // Node Size
                + 2 // Record Size
                + 2 // Depth
                + 1 // Split Percent
                + 1 // Merge Percent
                + sizeOfOffsets // Root Node Address
                + 2 // Number of Records in Root Node
                + sizeOfLengths // Total Number of Records in B-tree
                + 4; // Checksum
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize);
        Hdf5Utils.readBytes(channel, headerBuffer);
        return new BTreeV2Header(headerBuffer, sizeOfOffsets, sizeOfLengths);
    }
}

