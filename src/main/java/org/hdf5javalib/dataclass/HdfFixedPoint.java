package org.hdf5javalib.dataclass;

import lombok.Getter;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfReadUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.BitSet;

@Getter
public class HdfFixedPoint implements HdfData {
    private final byte[] bytes; // Stored in little-endian format by default unless specified otherwise
    private final FixedPointDatatype datatype;

    /**
     * Constructs an HdfFixedPoint from a byte array and a specified FixedPointDatatype.
     * <p>
     * This constructor initializes the HdfFixedPoint by storing a reference to the provided byte array
     * and associating it with the given datatype. The byte array length is validated against the size
     * specified by the datatype. The byte array is expected to represent a fixed-point number formatted
     * according to the datatype's specifications, including endianness.
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
     * This constructor converts the provided BigInteger value into a byte array with the specified size and
     * endianness as defined by the datatype, then delegates to the byte array constructor to initialize the
     * HdfFixedPoint. The value is formatted to fit the fixed-point representation specified by the datatype.
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

    public byte[] getBytes() {
        return bytes.clone();
    }

    public boolean isUndefined() {
        for(byte b : bytes) {
            if (b != (byte) 0xFF) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return datatype.getInstance(String.class, bytes);
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        HdfWriteUtils.writeFixedPointToBuffer(buffer, this);
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }
}