package org.hdf5javalib.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HdfTestWriteUtils {
    public static void compareByteArraysWithTimestampExclusion(byte[] javaBytes, byte[] cppBytes, int timestampOffset) throws java.io.IOException {
        if (javaBytes.length != cppBytes.length) {
            throw new AssertionError("Array lengths differ: Java=" + javaBytes.length + ", C++=" + cppBytes.length);
        }

        int diffOffset = Arrays.mismatch(javaBytes, cppBytes);
        if (diffOffset == -1 || (diffOffset >= timestampOffset && diffOffset < timestampOffset + 4)) {
            return; // No mismatch or only in timestamp
        }

        maskTimestamp(javaBytes, timestampOffset);
        maskTimestamp(cppBytes, timestampOffset);

        diffOffset = Arrays.mismatch(javaBytes, cppBytes);
        if (diffOffset != -1) {
            int windowSize = 64; // ±32 bytes
            int halfWindow = windowSize / 2; // 32
            int start = (diffOffset - halfWindow) & ~0xF; // Align to 16-byte boundary
            int end = start + windowSize;

            if (start < 0 || end > javaBytes.length) {
                throw new IllegalStateException("Dump range out of bounds: " + start + " to " + end);
            }

            byte[] javaWindow = Arrays.copyOfRange(javaBytes, start, end);
            byte[] cppWindow = Arrays.copyOfRange(cppBytes, start, end);

            System.out.println("Difference found at offset: 0x" + Integer.toHexString(diffOffset).toUpperCase());
            System.out.println("Java bytes (masked):");
            HdfDebugUtils.dumpByteBuffer(ByteBuffer.wrap(javaWindow), start); // Pass file offset
            System.out.println("C++ bytes (masked):");
            HdfDebugUtils.dumpByteBuffer(ByteBuffer.wrap(cppWindow), start); // Pass file offset

            throw new AssertionError("Byte arrays differ at offset 0x" + Integer.toHexString(diffOffset).toUpperCase() + " (excluding timestamp)");
        }
    }

    public static void maskTimestamp(byte[] bytes, int offset) {
        for (int i = 0; i < 4; i++) { // 4-byte timestamp
            bytes[offset + i] = 0;    // Modify in place
        }
    }
}