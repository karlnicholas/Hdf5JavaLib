package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

class FractalHeapDirectBlock {
    public final String signature;
    public final short version;
    public final long heapHeaderAddress;
    public final long blockOffset;
    public final byte[] objectData;
    public final Integer checksum;

    private FractalHeapDirectBlock(ByteBuffer bb, FractalHeapHeader header, int sizeOfOffsets) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        this.signature = Hdf5Utils.readSignature(bb, "FHDB");
        this.version = bb.get();
        this.heapHeaderAddress = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        int blockOffsetSize = Hdf5Utils.ceilLog2(1L << header.maximumHeapSize) / 8;
        this.blockOffset = Hdf5Utils.readVariableSizeUnsigned(bb, blockOffsetSize);
        if ((header.flags & 2) != 0) {
//            dataSize -= 4;
//            bb.position(bb.position() + dataSize);
            this.checksum = bb.getInt();
//            bb.position(bb.position() - dataSize - 4);
        } else {
            this.checksum = null;
        }
        this.objectData = bb.array();
    }

    public static FractalHeapDirectBlock read(SeekableByteChannel channel, long blockAddress, long expectedBlockSize, FractalHeapHeader header, int sizeOfOffsets) throws IOException {
        channel.position(blockAddress);
        ByteBuffer blockBuffer = ByteBuffer.allocate((int) expectedBlockSize);
        Hdf5Utils.readBytes(channel, blockBuffer);
        return new FractalHeapDirectBlock(blockBuffer, header, sizeOfOffsets);
    }
}
