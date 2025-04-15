package org.hdf5javalib.utils;

import java.nio.ByteBuffer;

public class HdfDebugUtils {
    public static void dumpByteBuffer(ByteBuffer buffer, long fileOffset) {
        int bytesPerLine = 16; // 16 bytes per row
        int limit = buffer.limit();
        buffer.rewind(); // Reset position to 0 before reading

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < limit; i += bytesPerLine) {
            // Print the address (file offset + i in hex)
            sb.append(String.format("%08X:  ", fileOffset + i));

            StringBuilder ascii = new StringBuilder();

            // Print the first 8 bytes (hex values)
            for (int j = 0; j < 8; j++) {
                buildHexValues(buffer, limit, sb, i, ascii, j);
            }

            sb.append(" "); // Space separator

            // Print the second 8 bytes (hex values)
            for (int j = 8; j < bytesPerLine; j++) {
                buildHexValues(buffer, limit, sb, i, ascii, j);
            }

            // Append ASCII representation
            sb.append("  ").append(ascii);

            // Newline for next row
            sb.append("\n");
        }

        System.out.print(sb);
    }

    private static void buildHexValues(ByteBuffer buffer, int limit, StringBuilder sb, int i, StringBuilder ascii, int j) {
        if (i + j < limit) {
            byte b = buffer.get(i + j);
            sb.append(String.format("%02X ", b));
            ascii.append(isPrintable(b) ? (char) b : '.');
        } else {
            sb.append("   "); // Padding for incomplete lines
            ascii.append(" ");
        }
    }

    // Helper method to check if a byte is a printable ASCII character (excluding control chars)
    private static boolean isPrintable(byte b) {
        return (b >= 32 && b <= 126); // Includes extended ASCII
    }
}