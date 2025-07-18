package org.hdf5javalib.maydo.utils;

import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class DeflateByteBufferDeflater implements ByteBufferDeflater {
    private final Deflater deflater;

    public DeflateByteBufferDeflater(int compressionLevel) {
        if (compressionLevel < Deflater.NO_COMPRESSION || compressionLevel > Deflater.BEST_COMPRESSION) {
            throw new IllegalArgumentException("Invalid compression level: " + compressionLevel);
        }
        this.deflater = new Deflater(compressionLevel, true);  // nowrap=true for pure DEFLATE
    }

    /**
     * Deflates the contents of the input ByteBuffer using pure DEFLATE (raw stream, no wrapper).
     * The input ByteBuffer remains unchanged.
     * @param input The ByteBuffer to deflate (position to limit).
     * @return A new ByteBuffer containing the DEFLATE-compressed data.
     * @throws IOException If an I/O error occurs during deflation.
     */
    public ByteBuffer deflate(ByteBuffer input) throws IOException {
        deflater.reset();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Prepare input data reference (zero-copy if possible)
        byte[] inputArray;
        int offset;
        int length = input.remaining();
        if (input.hasArray()) {
            inputArray = input.array();
            offset = input.arrayOffset() + input.position();
        } else {
            // Fallback to copy for direct buffers
            ByteBuffer dup = input.duplicate();
            inputArray = new byte[length];
            dup.get(inputArray);
            offset = 0;
        }

        // Set Deflater input
        deflater.setInput(inputArray, offset, length);
        deflater.finish();

        // Deflate loop
        byte[] buffer = new byte[4096];  // Adjustable buffer size
        while (!deflater.finished()) {
            int deflatedBytes = deflater.deflate(buffer);
            baos.write(buffer, 0, deflatedBytes);
        }

        // Return as ByteBuffer
        return ByteBuffer.wrap(baos.toByteArray());
    }
}