package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.StringJoiner;

/**
 * Represents a Fractal Heap structure in an HDF5 file.
 * The primary entry point is the static read() method.
 */
public class FractalHeap {

    public final FractalHeapHeader header;
    public final Block rootBlock;

    private FractalHeap(FractalHeapHeader header, Block rootBlock) {
        this.header = header;
        this.rootBlock = rootBlock;
    }

    /**
     * Reads an entire Fractal Heap (header and all blocks) from a SeekableByteChannel.
     * This method orchestrates the reading of the header and the recursive reading
     * of the block structure (both Indirect and Direct blocks).
     *
     * @param channel The channel to read from, positioned at the start of the file.
     * @param position The absolute address of the Fractal Heap Header.
     * @param sizeOfOffsets The size of addresses in the HDF5 file (e.g., 8 for 64-bit).
     * @param sizeOfLengths The size of lengths in the HDF5 file (e.g., 8 for 64-bit).
     * @return A fully instantiated FractalHeap object containing the header and a tree of blocks.
     * @throws IOException If there is an error reading the file or the format is invalid.
     */
    public static FractalHeap read(SeekableByteChannel channel, long position, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        FractalHeapHeader header = FractalHeapHeader.read(channel, position, sizeOfOffsets, sizeOfLengths);
        Block rootBlock = null;

        long undefinedAddr = (sizeOfOffsets == 8) ? -1L : (1L << (sizeOfOffsets * 8)) - 1;

        if (header.rootBlockAddress != undefinedAddr && header.rootBlockAddress != 0) {
            // The root block is always at depth 0.
            if (header.currentRowsInRoot == 0) {
                // Root is a Direct Block.
                rootBlock = DirectBlock.read(channel, header.rootBlockAddress, header, 0);
            } else {
                // Root is an Indirect Block.
                rootBlock = IndirectBlock.read(channel, header.rootBlockAddress, header, 0);
            }
        }
        return new FractalHeap(header, rootBlock);
    }

    @Override
    public String toString() {
        return new StringJoiner("\n", FractalHeap.class.getSimpleName() + "[\n", "\n]")
                .add("  header=" + header)
                .add("  rootBlock=" + (rootBlock != null ? rootBlock.toString() : "null"))
                .toString();
    }
}