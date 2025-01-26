package com.github.karlnicholas.hdf5javalib.numeric;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class TestHdfFixedPoint {

    @Test
    public void testSigned32BitBigEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(65536).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, true);
        assertEquals(new BigInteger("65536"), fixedPoint.getBigIntegerValue());
    }

    @Test
    public void testSigned32BitLittleEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(65536).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, true);
        assertEquals(new BigInteger("65536"), fixedPoint.getBigIntegerValue());
    }

    @Test
    public void testNegativeSigned32BitLittleEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(-1).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, true);
        assertEquals(new BigInteger("-1"), fixedPoint.getBigIntegerValue());
    }

    @Test
    public void testUnsigned32BitBigEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0xFFFEFFFF).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);
        assertEquals(new BigInteger("4294901759"), fixedPoint.getBigIntegerValue());
    }

    @Test
    public void testUnsigned32BitLittleEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(0xFFFEFFFF).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);
        assertEquals(new BigInteger("4294901759"), fixedPoint.getBigIntegerValue());
    }

    @Test
    public void testGetHdfBytesBigEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(0xFFFEFFFF).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);
        byte[] hdfBytes = fixedPoint.getHdfBytes(false); // Request big-endian bytes
        assertArrayEquals(new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFF, (byte) 0xFF}, hdfBytes);
    }

    @Test
    public void testGetHdfBytesLittleEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(0xFFFEFFFF).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);
        byte[] hdfBytes = fixedPoint.getHdfBytes(true); // Request little-endian bytes
        assertArrayEquals(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFE, (byte) 0xFF}, hdfBytes);
    }

    @Test
    public void testGetHdfBytesSwitchToBigEndian() {
        ByteBuffer buffer = ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(0xFFFEFFFF).flip();
        HdfFixedPoint fixedPoint = HdfFixedPoint.readFromByteBuffer(buffer, 4, false);
        byte[] hdfBytes = fixedPoint.getHdfBytes(false); // Request big-endian bytes
        assertArrayEquals(new byte[]{(byte) 0xFF, (byte) 0xFE, (byte) 0xFF, (byte) 0xFF}, hdfBytes);
    }

    @Test
    public void testBigIntegerConstructorLittleEndian() {
        HdfFixedPoint fixedPoint = new HdfFixedPoint(BigInteger.valueOf(123456), true);
        assertEquals(new BigInteger("123456"), fixedPoint.getBigIntegerValue());
        byte[] expectedLittleEndian = new byte[]{0x40, (byte) 0xE2, 0x01, 0x00}; // Little-endian bytes
        assertArrayEquals(expectedLittleEndian, fixedPoint.getHdfBytes(true));
    }

    @Test
    public void testBigIntegerConstructorBigEndian() {
        HdfFixedPoint fixedPoint = new HdfFixedPoint(BigInteger.valueOf(123456), false);
        assertEquals(new BigInteger("123456"), fixedPoint.getBigIntegerValue());
        byte[] expectedBigEndian = new byte[]{0x00, 0x01, (byte) 0xE2, 0x40}; // Big-endian bytes
        assertArrayEquals(expectedBigEndian, fixedPoint.getHdfBytes(false));
    }
}
