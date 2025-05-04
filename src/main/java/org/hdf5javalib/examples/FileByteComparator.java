package org.hdf5javalib.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * A utility class for comparing two files byte-by-byte.
 * <p>
 * The {@code FileByteComparator} class reads two files specified as command-line
 * arguments and compares their contents byte-by-byte. It outputs any differences
 * found, including byte values at differing offsets and differences in file lengths.
 * This tool is useful for verifying the integrity or equivalence of binary files,
 * such as HDF5 files.
 * </p>
 */
public class FileByteComparator {
    /**
     * Compares two files byte-by-byte and prints differences.
     * <p>
     * Expects two command-line arguments specifying the paths to the files to compare.
     * Outputs a table of differing bytes with their offsets and values in hexadecimal,
     * along with any length differences. If no differences are found, a message
     * indicates the files are identical.
     * </p>
     *
     * @param args command-line arguments: {@code args[0]} is the path to the first file,
     *             {@code args[1]} is the path to the second file
     */
    public static void main(String[] args) {
        // Check if two file paths are provided as command-line arguments
        if (args.length != 2) {
            System.out.println("Usage: java FileByteComparator <file1> <file2>");
            System.exit(1);
        }

        String file1Path = args[0];
        String file2Path = args[1];

        try {
            // Read the files into byte arrays
            byte[] file1Bytes = Files.readAllBytes(Paths.get(file1Path));
            byte[] file2Bytes = Files.readAllBytes(Paths.get(file2Path));

            // Determine the length to compare (shortest file length to avoid out-of-bounds)
            int minLength = Math.min(file1Bytes.length, file2Bytes.length);

            // Compare bytes and track differences
            boolean differencesFound = false;
            System.out.println("Offset    File1 Value    File2 Value");
            System.out.println("------------------------------------");

            for (int i = 0; i < minLength; i++) {
                if (file1Bytes[i] != file2Bytes[i]) {
                    differencesFound = true;
                    // Print offset and byte values in hex
                    System.out.printf("0x%-8X  0x%02X          0x%02X%n",
                            i,
                            file1Bytes[i] & 0xFF,
                            file2Bytes[i] & 0xFF);
                }
            }

            // Check for length differences
            if (file1Bytes.length != file2Bytes.length) {
                differencesFound = true;
                System.out.printf("Files differ in length: File1 = %d bytes, File2 = %d bytes%n",
                        file1Bytes.length, file2Bytes.length);
                if (file1Bytes.length > file2Bytes.length) {
                    System.out.println("Extra bytes in File1 after offset 0x" +
                            Integer.toHexString(minLength - 1).toUpperCase() + ":");
                    for (int i = minLength; i < file1Bytes.length; i++) {
                        System.out.printf("0x%-8X  0x%02X%n", i, file1Bytes[i] & 0xFF);
                    }
                } else {
                    System.out.println("Extra bytes in File2 after offset 0x" +
                            Integer.toHexString(minLength - 1).toUpperCase() + ":");
                    for (int i = minLength; i < file2Bytes.length; i++) {
                        System.out.printf("0x%-8X  0x%02X%n", i, file2Bytes[i] & 0xFF);
                    }
                }
            }

            if (!differencesFound) {
                System.out.println("No differences found between the files.");
            }

        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
            System.exit(1);
        }
    }
}