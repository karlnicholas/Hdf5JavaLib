package org.hdf5javalib.redo.dataclass;

import org.hdf5javalib.redo.datatype.FixedPointDatatype;
import org.hdf5javalib.redo.utils.HdfReadUtils;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Represents an HDF5 fixed-point data structure.
 * <p>
 * The {@code HdfFixedPoint} class encapsulates a fixed-point number in an HDF5 file,
 * associating raw byte data with a {@link FixedPointDatatype} that defines the number's
 * size, endianness, and other properties. It implements the {@link HdfData} interface
 * to provide methods for accessing the data, checking for undefined values, and
 * converting it to various Java types.
 * </p>
 *
 * @see HdfData
 * @see FixedPointDatatype
 */
public class HdfFixedPoint implements HdfData, Comparable<HdfFixedPoint> {
    /** The raw byte array containing the fixed-point data. */
    private final byte[] bytes;
    /** The FixedPointDatatype defining the fixed-point structure, size, and format. */
    private final FixedPointDatatype datatype;

    /**
     * Constructs an HdfFixedPoint from a byte array and a specified FixedPointDatatype.
     * <p>
     * This constructor initializes the HdfFixedPoint by storing a reference to the provided
     * byte array and associating it with the given datatype. The byte array length is validated
     * against the size specified by the datatype. The byte array is expected to represent a
     * fixed-point number formatted according to the datatype's specifications, including
     * endianness, bit offset, and precision.
     * </p>
     *
     * @param bytes    the byte array containing the fixed-point data
     * @param datatype the FixedPointDatatype defining the fixed-point structure, size, and format
     * @throws IllegalArgumentException if the byte array length does not match the datatype's size
     * @throws NullPointerException     if either {@code bytes} or {@code datatype} is null
     */
    public HdfFixedPoint(byte[] bytes, FixedPointDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
        this.bytes = bytes;
        this.datatype = datatype;
    }

    /**
     * Constructs an HdfFixedPoint from a BigInteger value and a specified FixedPointDatatype.
     * <p>
     * This constructor converts the provided BigInteger value into a byte array with the
     * specified size and endianness as defined by the datatype, then delegates to the byte
     * array constructor to initialize the HdfFixedPoint. The value is formatted to fit the
     * fixed-point representation specified by the datatype, including bit precision and padding.
     * </p>
     *
     * @param value    the BigInteger value representing the fixed-point number
     * @param datatype the FixedPointDatatype defining the fixed-point structure, size, and format
     * @throws IllegalArgumentException if the resulting byte array length does not match the datatype's size
     * @throws NullPointerException     if either {@code value} or {@code datatype} is null
     */
    public HdfFixedPoint(BigInteger value, FixedPointDatatype datatype) {
        this(toSizedByteArray(value, datatype.getSize(), datatype.isBigEndian()), datatype);
    }

    /**
     * Converts a BigInteger value to a byte array with the specified size and endianness.
     *
     * @param value     the BigInteger value to convert
     * @param byteSize  the desired size of the byte array
     * @param bigEndian true for big-endian, false for little-endian
     * @return a byte array representing the value
     */
    private static byte[] toSizedByteArray(BigInteger value, int byteSize, boolean bigEndian) {
        byte[] fullBytes = value.toByteArray();
        byte[] result = new byte[byteSize];
        int copyLength = Math.min(fullBytes.length, byteSize);
        System.arraycopy(fullBytes, fullBytes.length - copyLength, result, byteSize - copyLength, copyLength);
        if (!bigEndian) {
            HdfReadUtils.reverseBytesInPlace(result);
        }
        return result;
    }

    /**
     * Returns a copy of the byte array containing the fixed-point data.
     *
     * @return a cloned byte array to prevent external modification
     */
    public byte[] getBytes() {
        return bytes.clone();
    }

    /**
     * Checks if the fixed-point value is undefined.
     * <p>
     * A fixed-point value is considered undefined if all bytes are set to 0xFF,
     * as per the HDF5 specification for undefined values.
     * </p>
     *
     * @return true if the value is undefined, false otherwise
     */
    public boolean isUndefined() {
        for(byte b : bytes) {
            if (b != (byte) 0xFF) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a string representation of the fixed-point data.
     * <p>
     * The string representation is generated by delegating to the associated
     * {@code FixedPointDatatype}, which formats the byte data according to its
     * specifications (e.g., as a decimal or hexadecimal value).
     * </p>
     *
     * @return a string representation of the fixed-point value
     */
    @Override
    public String toString() {
        return datatype.getInstance(String.class, bytes);
    }

    /**
     * Writes the fixed-point data to the provided ByteBuffer.
     * <p>
     * This method delegates to {@link HdfWriteUtils#writeFixedPointToBuffer} to ensure
     * the byte data is written correctly, respecting the datatype's endianness and
     * other properties.
     * </p>
     *
     * @param buffer the ByteBuffer to write the byte data to
     */
    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        HdfWriteUtils.writeFixedPointToBuffer(buffer, this);
    }

    /**
     * Converts the fixed-point data to an instance of the specified Java class.
     * <p>
     * This method delegates to the associated {@code FixedPointDatatype} to perform
     * the conversion, allowing the data to be interpreted as the requested type
     * (e.g., {@link BigInteger}, {@link Long}, {@link String}, or other supported types).
     * </p>
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @return an instance of type T created from the byte data
     * @throws UnsupportedOperationException if the datatype cannot convert to the requested type
     */
    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }

    public FixedPointDatatype getDatatype() {
        return datatype;
    }

    @Override
    public int compareTo(HdfFixedPoint other) {
        if (other == null) {
            throw new NullPointerException("Cannot compare to null");
        }
        if (Objects.equals(datatype, other.datatype)) {
            // Compare bytes from MSB to LSB, treating as unsigned
            for (int i = bytes.length - 1; i >= 0; i--) {
                int thisByte = bytes[i] & 0xFF;
                int otherByte = other.bytes[i] & 0xFF;
                if (thisByte != otherByte) {
                    return Integer.compare(thisByte, otherByte);
                }
            }
            return 0;
        } else {
            // Handle undefined values
            boolean thisUndefined = isUndefined();
            boolean otherUndefined = other.isUndefined();
            if (thisUndefined && otherUndefined) {
                return 0; // Both undefined, considered equal
            }
            if (thisUndefined) {
                return 1; // Undefined is less than defined
            }
            if (otherUndefined) {
                return -1; // Defined is greater than undefined
            }

            // Convert byte arrays to BigInteger for comparison
            byte[] thisBytes = bytes.clone();
            byte[] otherBytes = other.getBytes();

            // Adjust for endianness if necessary
            if (!datatype.isBigEndian()) {
                HdfReadUtils.reverseBytesInPlace(thisBytes);
                HdfReadUtils.reverseBytesInPlace(otherBytes);
            }

            // Interpret bytes as BigInteger, considering signedness
            BigInteger thisValue = datatype.isSigned() ? new BigInteger(thisBytes) : new BigInteger(1, thisBytes);
            BigInteger otherValue = other.getDatatype().isSigned() ? new BigInteger(otherBytes) : new BigInteger(1, otherBytes);

            return thisValue.compareTo(otherValue);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HdfFixedPoint that = (HdfFixedPoint) o;
        return Objects.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bytes);
    }

    public byte[] add(HdfFixedPoint addend) {
        if (Objects.equals(datatype, addend.datatype)) {
            return addBytes(bytes, addend.bytes);
        } else {
            throw new UnsupportedOperationException("Addition requires identical datatype instances");
        }
    }

    public static byte[] addBytes(byte[] bytesFirst, byte[] bytesSecond) {
        if (bytesFirst.length != bytesSecond.length) {
            throw new IllegalArgumentException("Byte arrays must be the same length");
        }
        byte[] result = new byte[bytesFirst.length]; // Output size matches datatype
        int carry = 0; // Carry from previous sum (0 or 1)

        // Sum bytes in little-endian order
        for (int i = 0; i < bytesFirst.length; i++) {
            int thisByte = bytesFirst[i] & 0xFF; // Unsigned (0-255)
            int otherByte = bytesSecond[i] & 0xFF; // Unsigned (0-255)
            int sum = thisByte + otherByte + carry; // Sum of three bytes (0-511)
            result[i] = (byte) (sum & 0xFF); // Lower 8 bits to result
            carry = sum >>> 8; // Carry (0 or 1)
        }

        // Check for overflow (carry means sum exceeds datatype.getSize())
        if (carry > 0) {
            throw new ArithmeticException("Unsigned sum overflow");
        }

        return result;
    }

    public byte[] minus(HdfFixedPoint subtrahend) {
        if (Objects.equals(datatype, subtrahend.datatype)) {
            return minusBytes(bytes, subtrahend.bytes);
        } else {
            throw new UnsupportedOperationException("Subtraction requires identical datatype instances");
        }
    }

    public static byte[] minusBytes(byte[] firstBytes, byte[] secondBytes) {
        if(firstBytes.length != secondBytes.length) {
            throw new IllegalArgumentException("Byte arrays must be the same length");
        }
        byte[] result = new byte[firstBytes.length]; // Output size matches datatype
        int borrow = 0; // Borrow from previous subtraction (0 or 1)

        // Subtract bytes in little-endian order
        for (int i = 0; i < firstBytes.length; i++) {
            int thisByte = firstBytes[i] & 0xFF; // Unsigned (0-255)
            int otherByte = secondBytes[i] & 0xFF; // Unsigned (0-255)
            int diff = thisByte - otherByte - borrow; // Difference (-256 to 255)
            if (diff < 0) {
                diff += 256; // Adjust to 0-255
                borrow = 1; // Borrow for next position
            } else {
                borrow = 0; // No borrow needed
            }
            result[i] = (byte) (diff & 0xFF); // Store lower 8 bits
        }

        // Check for underflow (borrow means result is negative)
        if (borrow > 0) {
            throw new ArithmeticException("Unsigned subtraction underflow");
        }

        return result;
    }

    public byte[] minusOne() {
        return minusOneBytes(bytes);
    }

    public static byte[] minusOneBytes(byte[] bytes) {
        byte[] result = new byte[bytes.length]; // Output size matches datatype
        int borrow = 0; // Borrow from previous subtraction (0 or 1)

        // Subtract 1 in little-endian order
        for (int i = 0; i < bytes.length; i++) {
            int thisByte = bytes[i] & 0xFF; // Unsigned (0-255)
            int diff = thisByte - (i == 0 ? 1 : 0) - borrow; // Subtract 1 from LSB only
            if (diff < 0) {
                diff += 256; // Adjust to 0-255
                borrow = 1; // Borrow for next position
            } else {
                borrow = 0; // No borrow needed
            }
            result[i] = (byte) (diff & 0xFF); // Store lower 8 bits
        }

        // Check for underflow (borrow means result is negative)
        if (borrow > 0) {
            throw new ArithmeticException("Unsigned subtraction underflow");
        }
        return result;
    }

    public void mutate(byte[] bytes) {
        System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
    }

    public static byte[] truncateTo2048Boundary(byte[] bytes) {
        if (bytes == null || bytes.length == 0 || bytes.length > 8) {
            throw new IllegalArgumentException("Bytes must be 1-8 bytes");
        }
        byte[] result = new byte[bytes.length];
        System.arraycopy(bytes, 0, result, 0, bytes.length);
        // Clear lower 11 bits: last 3 bits of byte[1] and all 8 bits of byte[0]
        if (bytes.length > 0) {
            result[0] = 0; // Clear byte[0]
        }
        if (bytes.length > 1) {
            result[1] &= (byte)0xF8; // Clear lower 3 bits of byte[1]
        }
        return result;
    }

    @Override
    public HdfFixedPoint clone() {
        return new HdfFixedPoint(bytes.clone(), datatype);
    }
}