package org.hdf5javalib.maydo.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Fletcher32ByteBufferDeflater implements ByteBufferDeflater {
    public Fletcher32ByteBufferDeflater(int[] clientData) {
        // Client data not used for Fletcher-32 filter
    }

    /**
     * Applies the Fletcher-32 filter decoding: verifies the checksum on the input ByteBuffer
     * (data + 4-byte checksum) and returns the data without the checksum if valid.
     * The input ByteBuffer's position is advanced to its limit after processing.
     * @param input The ByteBuffer containing data + Fletcher-32 checksum (position to limit).
     * @return A new ByteBuffer containing the verified data.
     * @throws IOException If an I/O error occurs or checksum mismatch.
     */
    public ByteBuffer deflate(ByteBuffer input) throws IOException {
        int length = input.remaining();
        if (length < 4) {
            throw new IOException("Input too short for Fletcher-32 checksum");
        }

        // Assume input has array since read from file and position is 0
        byte[] dataArray = input.array();
        int offset = input.arrayOffset();
        int dataLength = length - 4;

        // Compute Fletcher-32 on data part
        Fletcher32 checksumCalc = new Fletcher32();
        checksumCalc.update(dataArray, offset, dataLength);
        long computed = checksumCalc.getValue();

        // Read stored checksum (as two big-endian 16-bit values: c0 then c1)
        input.order(ByteOrder.BIG_ENDIAN);
        input.position(dataLength);
        long c0_stored = input.getShort() & 0xFFFFL;
        long c1_stored = input.getShort() & 0xFFFFL;
        long stored = (c1_stored << 16) | c0_stored;

        // Verify
        if (computed != stored) {
            throw new IOException("Fletcher-32 checksum mismatch: computed " + Long.toHexString(computed) + ", stored " + Long.toHexString(stored));
        }

        // Position is now at length due to getShort calls, so input is advanced

        // Return data as ByteBuffer
        return ByteBuffer.wrap(dataArray, offset, dataLength);
    }
}