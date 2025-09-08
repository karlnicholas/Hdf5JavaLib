package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

class FractalHeapIndirectBlock {
    public final String signature;
    public final long[] blockOffsets; // <-- ADDED THIS FIELD
    public final long[] childDirectBlockAddresses;
    public final long[] childIndirectBlockAddresses;

    // REFACTORED: Constructor now takes the specific number of rows for this block
    private FractalHeapIndirectBlock(ByteBuffer bb, int numberOfRows, FractalHeapHeader header, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);
        this.signature = Hdf5Utils.readSignature(bb, "FHIB");
        bb.get(); // Version
        Hdf5Utils.readOffset(bb, sizeOfOffsets); // Heap Header Addr

        int log2MaxDirect = Hdf5Utils.ceilLog2(header.maximumDirectBlockSize);
        int log2Start = Hdf5Utils.ceilLog2(header.startingBlockSize);
        int maxDblockRows = (log2MaxDirect - log2Start) + 2;

        int K, N; // K = Direct children, N = Indirect children
        if (numberOfRows <= maxDblockRows) {
            K = numberOfRows * header.tableWidth;
            N = 0;
        } else {
            K = maxDblockRows * header.tableWidth;
            N = (numberOfRows - maxDblockRows) * header.tableWidth;
        }

        // *** START OF MAJOR CHANGE ***
        // An indirect block stores the starting offset of EACH child block.
        // The size of these offsets is given by 'sizeOfLengths'.
        this.blockOffsets = new long[K + N];
        for (int i = 0; i < this.blockOffsets.length; i++) {
            // Use sizeOfLengths, not a calculated blockOffsetSize
            this.blockOffsets[i] = Hdf5Utils.readVariableSizeUnsigned(bb, sizeOfLengths);
        }
        // *** END OF MAJOR CHANGE ***

        this.childDirectBlockAddresses = new long[K];
        for (int i = 0; i < K; i++) {
            this.childDirectBlockAddresses[i] = Hdf5Utils.readOffset(bb, sizeOfOffsets);
            // This filter logic seems to be for a different object model (B-Tree v2),
            // but we'll keep it as it was in your original code.
            // For standard fractal heaps, this part is usually simpler.
            if (header.ioFiltersEncodedLength > 0) {
                bb.position(bb.position() + sizeOfLengths + 4); // Skip block size and filter mask
            }
        }

        this.childIndirectBlockAddresses = new long[N];
        for (int i = 0; i < N; i++) {
            this.childIndirectBlockAddresses[i] = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        }
        // Checksum might be here at the end if the flag is set
    }

    // REFACTORED: Read method now takes the specific number of rows for this block
    public static FractalHeapIndirectBlock read(SeekableByteChannel channel, long blockAddress, int numberOfRows, FractalHeapHeader header, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        int log2MaxDirect = Hdf5Utils.ceilLog2(header.maximumDirectBlockSize);
        int log2Start = Hdf5Utils.ceilLog2(header.startingBlockSize);
        int maxDblockRows = (log2MaxDirect - log2Start) + 2;

        int numDirectEntries = Math.min(numberOfRows, maxDblockRows) * header.tableWidth;
        int numIndirectEntries = (numberOfRows > maxDblockRows) ? (numberOfRows - maxDblockRows) * header.tableWidth : 0;
        int totalEntries = numDirectEntries + numIndirectEntries;

        // Base size of header
        long calculatedBlockSize = 4 + 1 + sizeOfOffsets; // Signature, Version, Header Addr

        // *** START OF MAJOR CHANGE IN SIZE CALCULATION ***
        // Add size of the block offsets array
        calculatedBlockSize += (long) totalEntries * sizeOfLengths;
        // *** END OF MAJOR CHANGE IN SIZE CALCULATION ***

        // Size of direct block entries
        int directEntrySize = sizeOfOffsets;
        if (header.ioFiltersEncodedLength > 0) {
            directEntrySize += sizeOfLengths + 4;
        }
        calculatedBlockSize += (long) numDirectEntries * directEntrySize;

        // Size of indirect block entries
        calculatedBlockSize += (long) numIndirectEntries * sizeOfOffsets;

        // Add checksum if present
        if ((header.flags & 2) != 0) {
            calculatedBlockSize += 4;
        }

        channel.position(blockAddress);
        // Ensure calculated size doesn't exceed Integer.MAX_VALUE for ByteBuffer allocation
        if (calculatedBlockSize > Integer.MAX_VALUE) {
            throw new IOException("Fractal Heap Indirect Block is too large to read into memory.");
        }
        ByteBuffer blockBuffer = ByteBuffer.allocate((int) calculatedBlockSize);
        Hdf5Utils.readBytes(channel, blockBuffer);

        return new FractalHeapIndirectBlock(blockBuffer, numberOfRows, header, sizeOfOffsets, sizeOfLengths);
    }
}