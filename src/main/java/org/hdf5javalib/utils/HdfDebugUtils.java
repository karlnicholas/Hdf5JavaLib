package org.hdf5javalib.utils;

import java.nio.ByteBuffer;

/**
 * Utility class for debugging HDF5 file data by dumping the contents of a ByteBuffer.
 * <p>
 * The {@code HdfDebugUtils} class provides methods to print the contents of a {@link ByteBuffer}
 * in a hexadecimal and ASCII format, useful for inspecting raw data in HDF5 files during debugging.
 * The output is formatted with 16 bytes per line, showing both hex values and printable ASCII characters.
 * </p>
 */
public class HdfDebugUtils {
    /**
     * Dumps the contents of a ByteBuffer to the console in a hex and ASCII format.
     * <p>
     * Each line displays 16 bytes, with the file offset in hexadecimal, followed by the hex values
     * of the bytes, and their ASCII representation (printable characters or '.' for non-printable).
     * The buffer's position is reset to 0 before dumping and remains unchanged after the operation.
     * </p>
     *
     * @param buffer     the ByteBuffer containing the data to dump
     * @param fileOffset the starting file offset for display purposes
     */
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

    /**
     * Builds hex and ASCII representations for a single byte in the buffer.
     *
     * @param buffer the ByteBuffer containing the data
     * @param limit  the limit of the buffer
     * @param sb     the StringBuilder for hex output
     * @param i      the base index in the buffer
     * @param ascii  the StringBuilder for ASCII output
     * @param j      the offset within the current line
     */
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

    /**
     * Checks if a byte represents a printable ASCII character.
     *
     * @param b the byte to check
     * @return true if the byte is a printable ASCII character (32 to 126), false otherwise
     */
    private static boolean isPrintable(byte b) {
        return (b >= 32 && b <= 126); // Includes extended ASCII
    }
}