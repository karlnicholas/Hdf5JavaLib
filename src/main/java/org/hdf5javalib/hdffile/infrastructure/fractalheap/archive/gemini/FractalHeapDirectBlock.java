package org.hdf5javalib.hdffile.infrastructure.fractalheap.archive.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

// package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;
// ... (imports)

class FractalHeapDirectBlock {
    public final String signature;
    public final short version;
    public final long heapHeaderAddress;
    public final long blockOffset;
    public final byte[] objectData; // Now contains ONLY the object data
    public final Integer checksum;

    private FractalHeapDirectBlock(ByteBuffer bb, FractalHeapHeader header, int sizeOfOffsets) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        this.signature = Hdf5Utils.readSignature(bb, "FHDB");
        this.version = bb.get();
        this.heapHeaderAddress = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        int blockOffsetSize = (Hdf5Utils.ceilLog2(1L << header.maximumHeapSize) + 7) / 8;
        this.blockOffset = Hdf5Utils.readVariableSizeUnsigned(bb, blockOffsetSize);

        // ### REASONING FOR CHANGE ###
        // The checksum appears at the END of the data. We need to handle this carefully.
        // We will copy the data between the header and the potential checksum into our objectData array.
        int dataSize = bb.remaining();
        if ((header.flags & 2) != 0) {
            // If checksum exists, it's the last 4 bytes.
            dataSize -= 4;
            this.checksum = bb.getInt(bb.position() + dataSize);
        } else {
            this.checksum = null;
        }

        // OLD: this.objectData = bb.array();
        // NEW: Copy only the remaining bytes (the actual data payload) into the objectData field.
        this.objectData = new byte[dataSize];
        bb.get(this.objectData);
    }

    public static FractalHeapDirectBlock read(SeekableByteChannel channel, long blockAddress, long expectedBlockSize, FractalHeapHeader header, int sizeOfOffsets) throws IOException {
        channel.position(blockAddress);
        ByteBuffer blockBuffer = ByteBuffer.allocate((int) expectedBlockSize);
        Hdf5Utils.readBytes(channel, blockBuffer);
        return new FractalHeapDirectBlock(blockBuffer, header, sizeOfOffsets);
    }
}
