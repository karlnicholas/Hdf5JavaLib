package com.github.karlnicholas.hdf5javalib.numeric;

import com.github.karlnicholas.hdf5javalib.data.HdfFloatPoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestHdfFloatPoint {

    @Test
    public void testFloat32BigEndian() {
        byte[] bytes = new byte[]{(byte) 0x40, (byte) 0x49, (byte) 0x0F, (byte) 0xDB};
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)32, false); // Big-endian
        assertEquals(3.1415927f, floatPoint.getFloatValue(), 0.0000001f);
    }

    @Test
    public void testFloat32LittleEndian() {
        byte[] bytes = new byte[]{(byte) 0xDB, (byte) 0x0F, (byte) 0x49, (byte) 0x40}; // Little-endian representation
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)32, true); // Little-endian
        assertEquals(3.1415927f, floatPoint.getFloatValue(), 0.0000001f);
    }

    @Test
    public void testDouble64BigEndian() {
        byte[] bytes = new byte[]{(byte) 0x40, (byte) 0x09, (byte) 0x21, (byte) 0xFB, (byte) 0x54, (byte) 0x44, (byte) 0x2D, (byte) 0x18};
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)64, false); // Big-endian
        assertEquals(3.141592653589793, floatPoint.getDoubleValue(), 0.000000000000001);
    }

    @Test
    public void testDouble64LittleEndian() {
        byte[] bytes = new byte[]{(byte) 0x18, (byte) 0x2D, (byte) 0x44, (byte) 0x54, (byte) 0xFB, (byte) 0x21, (byte) 0x09, (byte) 0x40}; // Little-endian
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)64, true); // Little-endian
        assertEquals(3.141592653589793, floatPoint.getDoubleValue(), 0.000000000000001);
    }

    @Test
    public void testDefaultConstructorLittleEndian() {
        byte[] bytes = new byte[]{(byte) 0xDB, (byte) 0x0F, (byte) 0x49, (byte) 0x40}; // Little-endian representation of 3.1415927f
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)32);
        assertEquals(3.1415927f, floatPoint.getFloatValue(), 0.0000001f,
                "Default constructor should use little-endian.");
    }

    @Test
    public void testGetHdfBytesBigEndian() {
        byte[] bytes = new byte[]{(byte) 0x40, (byte) 0x09, (byte) 0x21, (byte) 0xFB, (byte) 0x54, (byte) 0x44, (byte) 0x2D, (byte) 0x18}; // Big-endian
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)64, false);
        byte[] hdfBytes = floatPoint.getHdfBytes(false); // Request big-endian bytes
        assertArrayEquals(bytes, hdfBytes, "HDF bytes for big-endian should match input bytes.");
    }

    @Test
    public void testGetHdfBytesLittleEndian() {
        byte[] bytes = new byte[]{(byte) 0x18, (byte) 0x2D, (byte) 0x44, (byte) 0x54, (byte) 0xFB, (byte) 0x21, (byte) 0x09, (byte) 0x40}; // Little-endian
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)64, true);
        byte[] hdfBytes = floatPoint.getHdfBytes(true); // Request little-endian bytes
        assertArrayEquals(bytes, hdfBytes, "HDF bytes for little-endian should match input bytes.");
    }

    @Test
    public void testGetHdfBytesSwitchToBigEndian() {
        byte[] bytes = new byte[]{(byte) 0x18, (byte) 0x2D, (byte) 0x44, (byte) 0x54, (byte) 0xFB, (byte) 0x21, (byte) 0x09, (byte) 0x40}; // Little-endian input
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)64, true);
        byte[] hdfBytes = floatPoint.getHdfBytes(false); // Request big-endian bytes
        assertArrayEquals(new byte[]{(byte) 0x40, (byte) 0x09, (byte) 0x21, (byte) 0xFB, (byte) 0x54, (byte) 0x44, (byte) 0x2D, (byte) 0x18}, hdfBytes,
                "HDF bytes should be reversed for big-endian output.");
    }

    @Test
    public void testGetHdfBytesSwitchToLittleEndian() {
        byte[] bytes = new byte[]{(byte) 0x40, (byte) 0x09, (byte) 0x21, (byte) 0xFB, (byte) 0x54, (byte) 0x44, (byte) 0x2D, (byte) 0x18}; // Big-endian input
        HdfFloatPoint floatPoint = new HdfFloatPoint(bytes, (short)64, false);
        byte[] hdfBytes = floatPoint.getHdfBytes(true); // Request little-endian bytes
        assertArrayEquals(new byte[]{(byte) 0x18, (byte) 0x2D, (byte) 0x44, (byte) 0x54, (byte) 0xFB, (byte) 0x21, (byte) 0x09, (byte) 0x40}, hdfBytes,
                "HDF bytes should be reversed for little-endian output.");
    }
}
