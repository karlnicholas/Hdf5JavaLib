package org.hdf5javalib.hdffile.infrastructure.fractalheap.archive.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

// package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;
// ... (imports)

class FractalHeapIndirectBlock {
    public final String signature;

    // ### REASONING FOR CHANGE ###
    // Changed from `long[] blockOffsets` to a single `long blockOffset`.
    // An indirect block's header contains ONE offset, which is the starting
    // linear address of the FIRST block it points to.
    public final long blockOffset;

    public final long[] childDirectBlockAddresses;
    public final long[] childIndirectBlockAddresses;

    private FractalHeapIndirectBlock(ByteBuffer bb, int numberOfRows, FractalHeapHeader header, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        this.signature = Hdf5Utils.readSignature(bb, "FHIB");
        bb.get(); // Version
        Hdf5Utils.readOffset(bb, sizeOfOffsets); // Heap Header Addr

        // ### REASONING FOR CHANGE ###
        // The size of the 'Block Offset' field is determined by the number of bits needed
        // to address the entire heap (maximumHeapSize is the number of bits).
        int sizeOfBlockOffsets = (header.maximumHeapSize + 7) / 8;
        this.blockOffset = Hdf5Utils.readVariableSizeUnsigned(bb, sizeOfBlockOffsets);

        // The rest of the logic for calculating K and N seems correct based on the spec
        // for handling "super blocks" that can point to both direct and other indirect blocks.
        int log2MaxDirect = Hdf5Utils.ceilLog2(header.maximumDirectBlockSize);
        int log2Start = Hdf5Utils.ceilLog2(header.startingBlockSize);
        int maxDblockRows = (log2MaxDirect - log2Start) + 2;

        int K = Math.min(numberOfRows, maxDblockRows) * header.tableWidth;
        int N = (numberOfRows > maxDblockRows) ? (numberOfRows - maxDblockRows) * header.tableWidth : 0;

        this.childDirectBlockAddresses = new long[K];
        for (int i = 0; i < K; i++) {
            this.childDirectBlockAddresses[i] = Hdf5Utils.readOffset(bb, sizeOfOffsets);
            if (header.ioFiltersEncodedLength > 0) {
                bb.position(bb.position() + sizeOfLengths + 4); // Skip block size and filter mask
            }
        }

        this.childIndirectBlockAddresses = new long[N];
        for (int i = 0; i < N; i++) {
            this.childIndirectBlockAddresses[i] = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        }
    }

    // The static `read` method also needs a small fix in its size calculation logic.
    public static FractalHeapIndirectBlock read(SeekableByteChannel channel, long blockAddress, int numberOfRows, FractalHeapHeader header, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        // ... (K and N calculation is the same)
        int log2MaxDirect = Hdf5Utils.ceilLog2(header.maximumDirectBlockSize);
        int log2Start = Hdf5Utils.ceilLog2(header.startingBlockSize);
        int maxDblockRows = (log2MaxDirect - log2Start) + 2;

        int numDirectEntries = Math.min(numberOfRows, maxDblockRows) * header.tableWidth;
        int numIndirectEntries = (numberOfRows > maxDblockRows) ? (numberOfRows - maxDblockRows) * header.tableWidth : 0;

        long calculatedBlockSize = 4 + 1 + sizeOfOffsets; // Signature, Version, Header Addr

        // ### REASONING FOR CHANGE ###
        // The block offset is a single field, not an array. Its size is calculated from maximumHeapSize.
        int sizeOfBlockOffsets = (header.maximumHeapSize + 7) / 8;
        calculatedBlockSize += sizeOfBlockOffsets;

        // ... (rest of the size calculation is likely correct)
        int directEntrySize = sizeOfOffsets;
        if (header.ioFiltersEncodedLength > 0) {
            directEntrySize += sizeOfLengths + 4;
        }
        calculatedBlockSize += (long) numDirectEntries * directEntrySize;
        calculatedBlockSize += (long) numIndirectEntries * sizeOfOffsets;
        if ((header.flags & 2) != 0) {
            calculatedBlockSize += 4;
        }

        channel.position(blockAddress);
        if (calculatedBlockSize > Integer.MAX_VALUE) {
            throw new IOException("Fractal Heap Indirect Block is too large to read into memory.");
        }

        ByteBuffer blockBuffer = ByteBuffer.allocate((int) calculatedBlockSize);
        Hdf5Utils.readBytes(channel, blockBuffer);
        return new FractalHeapIndirectBlock(blockBuffer, numberOfRows, header, sizeOfOffsets, sizeOfLengths);
    }
}