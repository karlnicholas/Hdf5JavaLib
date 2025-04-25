package org.hdf5javalib.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class HdfTestWriteUtils {
    public static void compareByteArraysWithTimestampExclusion(byte[] javaBytes, byte[] cppBytes, int[] timestampOffsets) throws IOException {
        if (javaBytes.length != cppBytes.length) {
            throw new AssertionError("Array lengths differ: Java=" + javaBytes.length + ", C++=" + cppBytes.length);
        }

        // Mask all timestamp regions upfront
        maskTimestamps(javaBytes, timestampOffsets);
        maskTimestamps(cppBytes, timestampOffsets);

        // Compare masked arrays
        int diffOffset = Arrays.mismatch(javaBytes, cppBytes);
        if (diffOffset != -1) {
            int windowSize = 64; // ±32 bytes
            int halfWindow = windowSize / 2;
            int start = (diffOffset - halfWindow) & ~0xF; // Align to 16-byte boundary
            int end = start + windowSize;

            if (start < 0 || end > javaBytes.length) {
                throw new IllegalStateException("Dump range out of bounds: " + start + " to " + end);
            }

            byte[] javaWindow = Arrays.copyOfRange(javaBytes, start, end);
            byte[] cppWindow = Arrays.copyOfRange(cppBytes, start, end);

            System.out.println("Difference found at offset: 0x" + Integer.toHexString(diffOffset).toUpperCase());
            System.out.println("Java bytes (masked):");
            HdfDebugUtils.dumpByteBuffer(ByteBuffer.wrap(javaWindow), start);
            System.out.println("C++ bytes (masked):");
            HdfDebugUtils.dumpByteBuffer(ByteBuffer.wrap(cppWindow), start);

            throw new AssertionError("Byte arrays differ at offset 0x" + Integer.toHexString(diffOffset).toUpperCase() + " (excluding timestamps)");
        }
    }

    public static void maskTimestamps(byte[] bytes, int[] offsets) {
        for (int offset : offsets) {
            for (int i = 0; i < 4; i++) { // 4-byte timestamp
                bytes[offset + i] = 0;    // Modify in place
            }
        }
    }
}