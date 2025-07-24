package org.hdf5javalib.utils;

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

        // Assume input has array since read from file and position is 0
        byte[] inputArray = input.array();
        int offset = input.arrayOffset();
        int length = input.remaining();

        // Set Inflater input
        inflater.setInput(inputArray, offset, length);

        // Inflate loop
        byte[] buffer = new byte[4096];  // Adjustable buffer size
        while (!inflater.finished()) {
            int inflatedBytes;
            try {
                inflatedBytes = inflater.inflate(buffer);
            } catch (DataFormatException e) {
                throw new IOException("Invalid zlib data format", e);
            }
            if (inflatedBytes == 0 && inflater.needsInput()) {
                throw new IOException("Unexpected end of zlib input");
            }
            baos.write(buffer, 0, inflatedBytes);
        }

        // Return as ByteBuffer
        return ByteBuffer.wrap(baos.toByteArray());
    }
}