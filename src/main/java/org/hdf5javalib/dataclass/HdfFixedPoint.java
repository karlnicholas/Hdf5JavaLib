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

    public HdfFixedPoint(byte[] bytes, FixedPointDatatype datatype) {
        this.bytes = bytes.clone();
        this.datatype = datatype;
        validateSize(datatype.getSize());
    }

    public HdfFixedPoint(BigInteger value, FixedPointDatatype datatype) {
        this(toSizedByteArray(value, datatype.getSize(), datatype.isBigEndian()), datatype);
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    /**
     * Construct 64-bit precision from long value
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

    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, int size) {
        validateSize(size);
        byte[] bytes = getLittleEndianBytes(buffer, size);
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                new BitSet(),
                (short) size, (short) 0, (short) (size * 8)
        );
        return new HdfFixedPoint(bytes, datatype);
    }

    public static HdfFixedPoint readFromByteBuffer(ByteBuffer buffer, FixedPointDatatype datatype) {
        validateSize(datatype.getSize());
        byte[] bytes = getLittleEndianBytes(buffer, datatype.getSize());
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