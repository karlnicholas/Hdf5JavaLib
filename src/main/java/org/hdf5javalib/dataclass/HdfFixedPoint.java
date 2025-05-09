package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfReadUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;

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
 * @see org.hdf5javalib.dataclass.HdfData
 * @see org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype
 */
public class HdfFixedPoint implements HdfData {
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
}