package com.github.karlnicholas.hdf5javalib.dataclass;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HdfFixedPointTest {

    private HdfFixedPoint createFixedPoint(byte[] bytes, int size, boolean signed, boolean littleEndian,
                                           short bitOffset, short bitPrecision, boolean hipad, boolean lopad) {
        return new HdfFixedPoint(bytes, size, littleEndian, lopad, hipad, signed, bitOffset, bitPrecision);
    }

    private HdfFixedPoint fromByteBuffer(int value, int size, boolean signed, boolean littleEndian) {
        ByteBuffer buffer = ByteBuffer.allocate(size).order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
        if (size == 1) buffer.put((byte) value);
        else if (size == 2) buffer.putShort((short) value);
        else buffer.putInt(value);
        buffer.flip();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return createFixedPoint(bytes, size, signed, littleEndian, (short) 0, (short) 0, false, false);
    }

    @Test
    public void testSpecific() {
        byte[] bytes = {0x00, (byte) 0x34, 0x7F, (byte) 0x9A};
        HdfFixedPoint myValue = createFixedPoint(bytes, 4, false, true, (short) 7, (short) 25, false, false);
        Assertions.assertEquals(new BigDecimal("20250216.00"), myValue.toBigDecimal(2).setScale(2, RoundingMode.HALF_UP));
    }

    @Test
    public void testZero() {
        HdfFixedPoint fp = fromByteBuffer(0, 4, true, false);
        Assertions.assertEquals(BigInteger.ZERO, fp.toBigInteger());
        Assertions.assertEquals(BigDecimal.ZERO.setScale(10), fp.toBigDecimal(10));
    }

    @Test
    public void testMaxSigned32BitBigEndian() {
        HdfFixedPoint fp = fromByteBuffer(0x7FFFFFFF, 4, true, false);
        Assertions.assertEquals(new BigInteger("2147483647"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("2147483647.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testMinSigned32BitBigEndian() {
        HdfFixedPoint fp = fromByteBuffer(0x80000000, 4, true, false);
        Assertions.assertEquals(new BigInteger("-2147483648"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("-2147483648.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testMaxUnsigned32BitBigEndian() {
        HdfFixedPoint fp = fromByteBuffer(-1, 4, false, false);
        Assertions.assertEquals(new BigInteger("4294967295"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("4294967295.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testMaxSigned32BitLittleEndian() {
        HdfFixedPoint fp = fromByteBuffer(0x7FFFFFFF, 4, true, true);
        Assertions.assertEquals(new BigInteger("2147483647"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("2147483647.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testMinSigned32BitLittleEndian() {
        HdfFixedPoint fp = fromByteBuffer(0x80000000, 4, true, true);
        Assertions.assertEquals(new BigInteger("-2147483648"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("-2147483648.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testMaxUnsigned32BitLittleEndian() {
        HdfFixedPoint fp = fromByteBuffer(-1, 4, false, true);
        Assertions.assertEquals(new BigInteger("4294967295"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("4294967295.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testBitPrecision8of32Signed() {
        byte[] bytes = {(byte) 0x12, (byte) 0x34, (byte) 0x56, (byte) 0x78};
        HdfFixedPoint fp = createFixedPoint(bytes, 4, true, false, (short) 0, (short) 8, false, false);
        Assertions.assertEquals(new BigInteger("18"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("18.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testBitPrecision8of32Unsigned() {
        byte[] bytes = {(byte) 0xFF, (byte) 0x34, (byte) 0x56, (byte) 0x78};
        HdfFixedPoint fp = createFixedPoint(bytes, 4, false, false, (short) 0, (short) 8, false, false);
        Assertions.assertEquals(new BigInteger("255"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("255.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testBitOffset8Signed() {
        HdfFixedPoint fpTemp = fromByteBuffer(0x12345678, 4, true, false);
        HdfFixedPoint fp = createFixedPoint(fpTemp.getBytes(), 4, true, false, (short) 8, (short) 0, false, false);
        Assertions.assertEquals(new BigInteger("305419896"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("1193046.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testBitOffset8Unsigned() {
        HdfFixedPoint fpTemp = fromByteBuffer(-1, 4, false, false);
        HdfFixedPoint fp = createFixedPoint(fpTemp.getBytes(), 4, false, false, (short) 8, (short) 0, false, false);
        Assertions.assertEquals(new BigInteger("4294967295"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("16777215.9960937500"), fp.toBigDecimal(10));
    }

    @Test
    public void testHiPadSigned() {
        byte[] bytes = {(byte) 0x12, (byte) 0x34};
        HdfFixedPoint fp = createFixedPoint(bytes, 2, true, false, (short) 0, (short) 8, true, false);
        Assertions.assertEquals(new BigInteger("-238"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("-238.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testLoPadSigned() {
        byte[] bytes = {(byte) 0x12, (byte) 0x34};
        HdfFixedPoint fp = createFixedPoint(bytes, 2, true, false, (short) 0, (short) 8, false, true);
        Assertions.assertEquals(new BigInteger("-409"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("-409.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testHiPadLoPadUnsigned() {
        byte[] bytes = {(byte) 0x12, (byte) 0x34};
        HdfFixedPoint fp = createFixedPoint(bytes, 2, false, false, (short) 0, (short) 8, true, true);
        Assertions.assertEquals(new BigInteger("65535"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("65535.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testAllFFUnsigned() {
        byte[] bytes = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        HdfFixedPoint fp = createFixedPoint(bytes, 4, false, false, (short) 0, (short) 0, false, false);
        Assertions.assertEquals(new BigInteger("4294967295"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("4294967295.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testAllFFSigned() {
        byte[] bytes = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        HdfFixedPoint fp = createFixedPoint(bytes, 4, true, false, (short) 0, (short) 0, false, false);
        Assertions.assertEquals(new BigInteger("-1"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("-1.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testSmallSizeSigned() {
        HdfFixedPoint fp = fromByteBuffer(0xFF, 1, true, false);
        Assertions.assertEquals(new BigInteger("-1"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("-1.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testSmallSizeUnsigned() {
        HdfFixedPoint fp = fromByteBuffer(0xFF, 1, false, false);
        Assertions.assertEquals(new BigInteger("255"), fp.toBigInteger());
        Assertions.assertEquals(new BigDecimal("255.0000000000"), fp.toBigDecimal(10));
    }

    @Test
    public void testInvalidBitOffset() {
        byte[] bytes = {(byte) 0x12, (byte) 0x34};
        HdfFixedPoint fp = createFixedPoint(bytes, 2, true, false, (short) -1, (short) 0, false, false);
        Assertions.assertThrows(IllegalArgumentException.class, fp::toBigInteger);
    }

    @Test
    public void testExcessiveBitPrecision() {
        byte[] bytes = {(byte) 0x12, (byte) 0x34};
        HdfFixedPoint fp = createFixedPoint(bytes, 2, true, false, (short) 0, (short) 17, false, false);
        Assertions.assertThrows(IllegalArgumentException.class, fp::toBigInteger);
    }
}