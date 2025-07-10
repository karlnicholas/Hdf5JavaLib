package org.hdf5javalib.maydo.utils;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.datatype.FixedPointDatatype;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class for reading data from HDF5 files.
 * <p>
 * The {@code HdfReadUtils} class provides methods to read integers, skip bytes,
 * reverse byte arrays, and read fixed-point values from a {@link SeekableByteChannel}
 * or {@link ByteBuffer}. It also includes methods to check for undefined values
 * and validate sizes, ensuring compatibility with the HDF5 file format specifications.
 * </p>
 */
public class HdfReadUtils {
    /**
     * Reads a 4-byte integer from a file channel in little-endian order.
     *
     * @param fileChannel the seekable byte channel to read from
     * @return the integer value read
     * @throws IOException if an I/O error occurs
     */
    public static int readIntFromFileChannel(SeekableByteChannel fileChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // Assume little-endian as per HDF5 spec
        fileChannel.read(buffer);
        buffer.flip();
        return buffer.getInt();
    }

    /**
     * Retrieves the file path for a resource.
     *
     * @param fileName the name of the resource file
     * @return the Path to the resource file
     */
    public static Path getResourcePath(String fileName) {
        String resourcePath = HdfReadUtils.class.getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    /**
     * Skips a specified number of bytes in the file channel.
     *
     * @param fileChannel the seekable byte channel to skip bytes in
     * @param bytesToSkip the number of bytes to skip
     * @throws IOException if an I/O error occurs
     */
    public static void skipBytes(SeekableByteChannel fileChannel, int bytesToSkip) throws IOException {
        fileChannel.position(fileChannel.position() + bytesToSkip);
    }

    /**
     * Reverses the order of bytes in an array in place.
     *
     * @param input the byte array to reverse
     */
    public static void reverseBytesInPlace(byte[] input) {
        int i = 0, j = input.length - 1;
        while (i < j) {
            byte temp = input[i];
            input[i] = input[j];
            input[j] = temp;
            i++;
            j--;
        }
    }

    public static String readNullTerminatedString(ByteBuffer buffer) {
        StringBuilder nameBuilder = new StringBuilder();
        byte b;
        while ((b = buffer.get()) != 0) {
            nameBuilder.append((char) b);
        }
        return nameBuilder.toString();
    }

    /**
     * Reads a fixed-point value from a ByteBuffer using the specified datatype.
     *
     * @param fixedPointDatatype the fixed-point datatype defining the format
     * @param buffer             the ByteBuffer to read from
     * @return the {@link HdfFixedPoint} value read
     */
    public static HdfFixedPoint readHdfFixedPointFromBuffer(
            FixedPointDatatype fixedPointDatatype,
            ByteBuffer buffer
    ) {
        byte[] bytes = new byte[fixedPointDatatype.getSize()];
        buffer.get(bytes);
        return fixedPointDatatype.getInstance(HdfFixedPoint.class, bytes);
    }

    /**
     * Reads a fixed-point value from a file channel using the specified datatype.
     *
     * @param fixedPointDatatype the fixed-point datatype defining the format
     * @param fileChannel        the seekable byte channel to read from
     * @return the {@link HdfFixedPoint} value read
     * @throws IOException if an I/O error occurs
     */
    public static HdfFixedPoint readHdfFixedPointFromFileChannel(
            FixedPointDatatype fixedPointDatatype,
            SeekableByteChannel fileChannel
    ) throws IOException {
        byte[] bytes = new byte[fixedPointDatatype.getSize()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        return new HdfFixedPoint(bytes, fixedPointDatatype);
    }

    /**
     * Checks if the next bytes in the buffer represent an undefined value (all 0xFF).
     * <p>
     * The buffer's position is restored after checking.
     * </p>
     *
     * @param buffer the ByteBuffer to check
     * @param size   the number of bytes to check
     * @return true if all bytes are 0xFF, false otherwise
     */
    public static boolean checkUndefined(ByteBuffer buffer, int size) {
        buffer.mark();
        byte[] undefinedBytes = new byte[size];
        buffer.get(undefinedBytes);
        buffer.reset();
        for (byte b : undefinedBytes) {
            if (b != (byte) 0xFF) {
                return false;
            }
        }
        return true;
    }

    /**
     * Validates that a size is between 1 and 8 bytes.
     *
     * @param size the size to validate
     * @throws IllegalArgumentException if the size is not between 1 and 8
     */
    public static void validateSize(int size) {
        if (size <= 0 || size > 8) {
            throw new IllegalArgumentException("Size must be between 1 and 8 bytes");
        }
    }
}