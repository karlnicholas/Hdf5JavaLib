package org.hdf5javalib.maydo.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ShuffleByteBufferDeflater implements ByteBufferDeflater {
    private final int elementSize;

    public ShuffleByteBufferDeflater(int[] clientData) {
        this.elementSize = clientData[0];  // Number of bytes per element
    }

    /**
     * Unshuffles the contents of the input ByteBuffer.
     * The input ByteBuffer remains unchanged.
     * @param input The ByteBuffer containing shuffled data (position to limit).
     * @return A new ByteBuffer containing the unshuffled data.
     * @throws IOException If an I/O error occurs or data size is invalid.
     */
    public ByteBuffer deflate(ByteBuffer input) throws IOException {
        int length = input.remaining();
        if (length % elementSize != 0) {
            throw new IOException("Input size not aligned with element size");
        }
        int numElements = length / elementSize;

        // Assume input has array since read from file and position is 0
        byte[] shuffledArray = input.array();
        int offset = input.arrayOffset();

        // Create unshuffled array
        byte[] unshuffled = new byte[length];

        // Unshuffle: Reconstruct original elements
        for (int i = 0; i < elementSize; i++) {
            for (int j = 0; j < numElements; j++) {
                unshuffled[j * elementSize + i] = shuffledArray[offset + i * numElements + j];
            }
        }

        // Return as ByteBuffer
        return ByteBuffer.wrap(unshuffled);
    }
}