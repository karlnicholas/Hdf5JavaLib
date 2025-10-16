package org.hdf5javalib.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger log = LoggerFactory.getLogger(FileByteComparator.class);

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
     * {@code args[1]} is the path to the second file
     */
    public static void main(String[] args) {
        // Check if two file paths are provided as command-line arguments
        if (args.length != 2) { // +1
            log.info("Usage: java FileByteComparator <file1> <file2>");
            System.exit(1);
        }

        String file1Path = args[0];
        String file2Path = args[1];
        boolean differencesFound = false;

        try { // +1 (try/catch penalty applies here)
            // Read the files into byte arrays
            byte[] file1Bytes = Files.readAllBytes(Paths.get(file1Path));
            byte[] file2Bytes = Files.readAllBytes(Paths.get(file2Path));

            // Determine the length to compare (shortest file length to avoid out-of-bounds)
            int minLength = Math.min(file1Bytes.length, file2Bytes.length);

            // Compare bytes and track differences (CC: 2)
            log.info("Offset    File1 Value    File2 Value");
            log.info("------------------------------------");

            for (int i = 0; i < minLength; i++) { // +1
                if (file1Bytes[i] != file2Bytes[i]) { // +1 (nesting 1)
                    differencesFound = true;
                    // Print offset and byte values in hex
                    log.info("0x{:08X}  0x{:02X}  0x{:02X}", i, file1Bytes[i] & 0xFF, file2Bytes[i] & 0xFF);
                }
            }

            // Check for length differences (CC: 1)
            if (file1Bytes.length != file2Bytes.length) { // +1
                differencesFound = true;
                handleLengthDifferences(file1Bytes, file2Bytes, minLength);
            }

            if (!differencesFound) { // +1
                log.info("No differences found between the files.");
            }

        } catch (IOException e) { // +1 (catch penalty)
            log.error("Error reading files: ", e);
            System.exit(1);
        }
    }

    /**
     * Handles and logs differences in file lengths.
     * CC: 4
     */
    private static void handleLengthDifferences(byte[] file1Bytes, byte[] file2Bytes, int minLength) {
        log.info("Files differ in length: File1 = {} bytes, File2 = {}", file1Bytes.length, file2Bytes.length);
        String offset = Integer.toHexString(minLength - 1).toUpperCase();

        if (file1Bytes.length > file2Bytes.length) { // +1
            log.info("Extra bytes in File1 after offset 0x{}:", offset);
            for (int i = minLength; i < file1Bytes.length; i++) { // +1
                log.info("0x{:08X}  0x{:02X}", i, file1Bytes[i] & 0xFF);
            }
        } else { // +1
            log.info("Extra bytes in File2 after offset 0x{}:", offset);
            for (int i = minLength; i < file2Bytes.length; i++) { // +1
                log.info("0x{:08X}  0x{:02X}", i, file2Bytes[i] & 0xFF);
            }
        }
    }
}