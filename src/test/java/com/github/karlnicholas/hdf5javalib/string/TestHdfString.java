package com.github.karlnicholas.hdf5javalib.string;

import com.github.karlnicholas.hdf5javalib.data.HdfString;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class TestHdfString {

    @Test
    public void testHdfMetadataConstructor() {
        byte[] bytes = new byte[]{65, 66, 67, 0}; // "ABC\0" in UTF-8
        HdfString hdfString = new HdfString(bytes, true, true);

        assertEquals("ABC", hdfString.getValue());
        assertArrayEquals(bytes, hdfString.getHdfBytes());
        assertTrue(hdfString.toString().contains("ABC"));
    }

    @Test
    public void testDefaultConstructor() {
        byte[] bytes = new byte[]{65, 66, 67, 0}; // "ABC\0" in UTF-8
        HdfString hdfString = new HdfString(bytes);

        assertEquals("ABC", hdfString.getValue());
        assertArrayEquals(bytes, hdfString.getHdfBytes());
    }

    @Test
    public void testJavaValueConstructorUtf8() {
        HdfString hdfString = new HdfString("Hello", true);

        assertEquals("Hello", hdfString.getValue());
        assertArrayEquals("Hello\0".getBytes(StandardCharsets.UTF_8), hdfString.getHdfBytes());
    }

    @Test
    public void testJavaValueConstructorAscii() {
        HdfString hdfString = new HdfString("Hello", false);

        assertEquals("Hello", hdfString.getValue());
        assertArrayEquals("Hello\0".getBytes(StandardCharsets.US_ASCII), hdfString.getHdfBytes());
    }

    @Test
    public void testNonNullTerminatedString() {
        byte[] bytes = new byte[]{65, 66, 67}; // "ABC" without null-termination
        HdfString hdfString = new HdfString(bytes, false, true);

        assertEquals("ABC", hdfString.getValue());
        assertArrayEquals(bytes, hdfString.getHdfBytes());
    }

    @Test
    public void testInvalidNullTerminatedString() {
        byte[] bytes = new byte[]{65, 66, 67}; // Not null-terminated
        assertThrows(IllegalArgumentException.class, () -> new HdfString(bytes, true, true));
    }

    @Test
    public void testEmptyString() {
        byte[] bytes = new byte[]{0}; // Null-terminated empty string
        HdfString hdfString = new HdfString(bytes, true, true);

        assertEquals("", hdfString.getValue());
        assertArrayEquals(bytes, hdfString.getHdfBytes());
    }

    @Test
    public void testToStringOutput() {
        byte[] bytes = new byte[]{65, 66, 67, 0}; // "ABC\0"
        HdfString hdfString = new HdfString(bytes, true, true);

        String toString = hdfString.toString();
        assertTrue(toString.contains("ABC"));
        assertTrue(toString.contains("true")); // Should include nullTerminated flag
        assertTrue(toString.contains("true")); // Should include utf8Encoding flag
    }

    @Test
    public void testImmutability() {
        byte[] bytes = new byte[]{65, 66, 67, 0}; // "ABC\0"
        HdfString hdfString = new HdfString(bytes, true, true);

        bytes[0] = 0; // Attempt to mutate original array
        assertEquals("ABC", hdfString.getValue()); // Value should remain unchanged
    }

    @Test
    public void testUtf8MultibyteCharacter() {
        String multibyteString = "こんにちは"; // "Hello" in Japanese
        HdfString hdfString = new HdfString(multibyteString, true);

        assertEquals(multibyteString, hdfString.getValue());
        assertArrayEquals((multibyteString + "\0").getBytes(StandardCharsets.UTF_8), hdfString.getHdfBytes());
    }
}
