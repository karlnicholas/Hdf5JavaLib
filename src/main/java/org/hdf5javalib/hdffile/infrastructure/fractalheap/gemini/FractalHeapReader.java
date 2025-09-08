package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

public class FractalHeapReader {

    private final SeekableByteChannel channel;
    private final int sizeOfOffsets;
    private final int sizeOfLengths;

    public FractalHeapReader(SeekableByteChannel channel, int sizeOfOffsets, int sizeOfLengths) {
        this.channel = channel;
        this.sizeOfOffsets = sizeOfOffsets;
        this.sizeOfLengths = sizeOfLengths;
    }

    public FractalHeap read() throws IOException {
        FractalHeapHeader header = FractalHeapHeader.read(channel, sizeOfOffsets, sizeOfLengths);

        // Simply return a new FractalHeap instance with the header and channel.
        // NO block reading is done here.
        return new FractalHeap(header, channel, sizeOfOffsets, sizeOfLengths);
    }
}