package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

public class HdfReadUtils {
    public static int readIntFromFileChannel(SeekableByteChannel fileChannel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN); // Assume little-endian as per HDF5 spec
        fileChannel.read(buffer);
        buffer.flip();
        return buffer.getInt();
    }

    public static void skipBytes(SeekableByteChannel fileChannel, int bytesToSkip) throws IOException {
        fileChannel.position(fileChannel.position() + bytesToSkip);
    }

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

    public static HdfFixedPoint readHdfFixedPointFromBuffer(
        FixedPointDatatype fixedPointDatatype,
        ByteBuffer buffer
    ) {
        byte[] bytes = new byte[fixedPointDatatype.getSize()];
        buffer.get(bytes);
        return fixedPointDatatype.getInstance(HdfFixedPoint.class, bytes);
    }

    public static HdfFixedPoint readHdfFixedPointFromFileChannel(
            FixedPointDatatype fixedPointDatatype,
            SeekableByteChannel fileChannel
    ) throws IOException {
        byte[] bytes = new byte[fixedPointDatatype.getSize()];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        return new HdfFixedPoint(bytes, fixedPointDatatype);
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

    public static void validateSize(int size) {
        if (size <= 0 || size > 8) {
            throw new IllegalArgumentException("Size must be between 1 and 8 bytes");
        }
    }

}
