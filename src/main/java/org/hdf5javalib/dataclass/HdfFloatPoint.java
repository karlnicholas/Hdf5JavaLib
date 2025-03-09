package org.hdf5javalib.dataclass;

import org.hdf5javalib.file.dataobject.message.datatype.FloatingPointDatatype;

import java.nio.ByteBuffer;

public class HdfFloatPoint<T> implements HdfData<T> {
    private final Class<T> clazz;
    private final byte[] bytes;
    private FloatingPointDatatype datatype;

    public HdfFloatPoint(Class<T> clazz, byte[] bytes, FloatingPointDatatype datatype) {
        this.clazz = clazz;
        this.bytes = bytes;
        this.datatype = datatype;
    }

//    public BigDecimal getBigDecimalValue() {
//        if (datatype.getSize() == 32) {
//            return BigDecimal.valueOf(getFloatValue());
//        } else {
//            return BigDecimal.valueOf(getDoubleValue());
//        }
//    }

//    public byte[] getHdfBytes(boolean desiredLittleEndian) {
//        if (desiredLittleEndian == littleEndian) {
//            return Arrays.copyOf(bytes, bytes.length);
//        }
//        return reverseBytes(bytes);
//    }
//
//    private ByteBuffer readBuffer() {
//        ByteBuffer buffer = ByteBuffer.wrap(bytes);
//        buffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
//        return buffer;
//    }
//
//    private byte[] reverseBytes(byte[] input) {
//        byte[] reversed = new byte[input.length];
//        for (int i = 0; i < input.length; i++) {
//            reversed[i] = input[input.length - i - 1];
//        }
//        return reversed;
//    }

    @Override
    public String toString() {
        return getInstance().toString();
    }

    @Override
    public int getSizeMessageData() {
        return (short)bytes.length;
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {

    }

    @Override
    public T getInstance() {
        return datatype.getInstance(clazz, bytes);
    }
}
