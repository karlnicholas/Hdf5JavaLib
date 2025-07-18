package org.hdf5javalib.maydo.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

public class ZlibByteBufferDeflater implements ByteBufferDeflater {
    private final Inflater inflater;

    public ZlibByteBufferDeflater(int[] clientData) {
        this.inflater = new Inflater(false);  // nowrap=false for zlib format
    }

    /**
     * Inflates (decompresses) the contents of the input ByteBuffer using zlib format.
     * The input ByteBuffer remains unchanged.
     * @param input The ByteBuffer containing zlib-compressed data (position to limit).
     * @return A new ByteBuffer containing the decompressed data.
     * @throws IOException If an I/O error occurs.
     */
    public ByteBuffer deflate(ByteBuffer input) throws IOException {
        inflater.reset();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Prepare input data reference (zero-copy if possible)
        byte[] inputArray;
        int offset;
        int length = input.remaining();
        if (input.hasArray()) {
            inputArray = input.array();
            offset = input.arrayOffset() + input.position();
        } else {
            ByteBuffer dup = input.duplicate();
            inputArray = new byte[length];
            dup.get(inputArray);
            offset = 0;
        }

        // Set Inflater input
        inflater.setInput(inputArray, offset, length);

        // Inflate loop
        byte[] buffer = new byte[4096];  // Adjustable buffer size
        while (!inflater.finished()) {
            int inflatedBytes = 0;
            try {
                inflatedBytes = inflater.inflate(buffer);
            } catch (DataFormatException e) {
                throw new RuntimeException(e);
            }
            baos.write(buffer, 0, inflatedBytes);
        }

        // Return as ByteBuffer
        return ByteBuffer.wrap(baos.toByteArray());
    }
}