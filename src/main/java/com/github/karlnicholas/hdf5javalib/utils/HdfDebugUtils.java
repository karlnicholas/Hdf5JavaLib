package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.datatype.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class HdfDebugUtils {
    public static void printData(FileChannel fileChannel, CompoundDatatype compoundDataType, long dataAddress, long dimension ) throws IOException {
        Object[] data = new Object[17];
        fileChannel.position(dataAddress);
        for ( int i=0; i <dimension; ++i) {
            ByteBuffer dataBuffer = ByteBuffer.allocate(compoundDataType.getSize()).order(ByteOrder.LITTLE_ENDIAN);
            fileChannel.read(dataBuffer);
            dataBuffer.flip();
            for ( int column = 0; column < compoundDataType.getNumberOfMembers(); ++column ) {
                HdfCompoundDatatypeMember member = compoundDataType.getMembers().get(column);
                dataBuffer.position(member.getOffset());
                if (member.getType() instanceof StringDatatype) {
                    data[column] = ((StringDatatype) member.getType()).getInstance(dataBuffer);
                } else if (member.getType() instanceof FixedPointDatatype) {
                    data[column] = ((FixedPointDatatype) member.getType()).getInstance(dataBuffer);
                } else if (member.getType() instanceof FloatingPointDatatype) {
                    data[column] = ((FloatingPointDatatype) member.getType()).getInstance();
                } else {
                    throw new UnsupportedOperationException("Member type " + member.getType() + " not yet implemented.");
                }
            }
            System.out.println(Arrays.toString(data));
        }

    }

    public static void dumpByteBuffer(ByteBuffer buffer) {
        int bytesPerLine = 16; // 16 bytes per row
        int limit = buffer.limit();
        buffer.rewind(); // Reset position to 0 before reading

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < limit; i += bytesPerLine) {
            // Print the address (memory offset in hex)
            sb.append(String.format("%08X:  ", i));

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
