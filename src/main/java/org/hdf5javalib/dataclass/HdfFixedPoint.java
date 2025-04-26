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
        validateSize(datatype.getSize());
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

    public byte[] getBytes() {
        return bytes.clone();
    }

    /**
     * Creates an HdfFixedPoint representing a 64-bit fixed-point number from a long value.
     * <p>
     * This static factory method constructs an HdfFixedPoint by converting the provided long value into an
     * 8-byte array in little-endian order. It uses a FixedPointDatatype with 64-bit precision, no offset, and
     * no special flags (e.g., signed, normalized, or reserved bits). The resulting HdfFixedPoint is suitable
     * for representing standard 64-bit integer values.
     * </p>
     *
     * @param value the long value to convert into a fixed-point representation
     * @return a new HdfFixedPoint instance encapsulating the 64-bit fixed-point number
     */
    public static HdfFixedPoint of(long value) {
        byte[] bArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                (short) 8, (short) 0, (short) 64
        );
        return new HdfFixedPoint(bArray, datatype);
    }


    public static HdfFixedPoint undefined(int size) {
        validateSize(size);
        byte[] undefinedBytes = new byte[size];
        Arrays.fill(undefinedBytes, (byte) 0xFF);
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, true),
                (short) size, (short) 0, (short) (size * 8)
        );
        return new HdfFixedPoint(undefinedBytes, datatype);
    }

    public static HdfFixedPoint undefined(ByteBuffer buffer, int size) {
        validateSize(size);
        byte[] undefinedBytes = new byte[size];
        buffer.get(undefinedBytes);
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, true),
                (short) size, (short) 0, (short) (size * 8)
        );
        return new HdfFixedPoint(undefinedBytes, datatype);
    }

    public static HdfFixedPoint readFromFileChannel(SeekableByteChannel fileChannel, int size, BitSet classBitField, short bitOffset, short bitPrecision) throws IOException {
        validateSize(size);
        byte[] bytes = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(classBitField.get(0) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                classBitField,
                (short) size, bitOffset, bitPrecision
        );
        return new HdfFixedPoint(bytes, datatype);
    }

    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, int size, BitSet classBitField, short bitOffset, short bitPrecision) {
        validateSize(size);
        byte[] bytes = getLittleEndianBytes(buffer, size);
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                classBitField,
                (short) size, bitOffset, bitPrecision
        );
        return new HdfFixedPoint(bytes, datatype);
    }

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

    private static byte[] getLittleEndianBytes(ByteBuffer buffer, int size) {
        validateSize(size);
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        if (buffer.order() == ByteOrder.BIG_ENDIAN) {
            HdfReadUtils.reverseBytesInPlace(bytes);
        }
        return bytes;
    }

    public boolean isUndefined() {
        for(byte b : bytes) {
            if (b != (byte) 0xFF) {
                return false;
            }
        }
        return true;
    }

    private static void validateSize(int size) {
        if (size <= 0 || size > 8) {
            throw new IllegalArgumentException("Size must be between 1 and 8 bytes");
        }
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