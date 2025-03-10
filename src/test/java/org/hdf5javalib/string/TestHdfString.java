package org.hdf5javalib.string;

public class TestHdfString {

//    @Test
//    public void testHdfMetadataConstructor() {
//        byte[] bytes = new byte[]{65, 66, 67, 0}; // "ABC\0" in UTF-8
//        HdfString hdfString = new HdfString(bytes, StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.UTF8));
//
//        assertEquals("ABC", hdfString.getValue());
//        byte[] newBytes = hdfString.getBytes();
//        assertArrayEquals(bytes, newBytes);
//        assertTrue(hdfString.toString().contains("ABC"));
//    }
//
//    @Test
//    public void testJavaValueConstructorAscii() {
//        HdfString hdfString = new HdfString("Hello".getBytes(StandardCharsets.US_ASCII), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII));
//
//        assertEquals("Hello", hdfString.getValue());
//        assertArrayEquals("Hello".getBytes(StandardCharsets.US_ASCII), hdfString.getBytes());
//    }
//
//    @Test
//    public void testNonNullTerminatedString() {
//        byte[] bytes = new byte[]{65, 66, 67}; // "ABC" without null-termination
//        HdfString hdfString = new HdfString(bytes, StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII));
//
//        assertEquals("ABC", hdfString.getValue());
//        assertArrayEquals(bytes, hdfString.getBytes());
//    }
//
////    @Test
////    public void testInvalidNullTerminatedString() {
////        byte[] bytes = new byte[]{65, 66, 67}; // Not null-terminated
////        assertThrows(IllegalArgumentException.class, () -> new HdfString(bytes, StringDatatype.getStringTypeBitSet(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII)));
////    }
////
//    @Test
//    public void testEmptyString() {
//        byte[] bytes = new byte[]{0}; // Null-terminated empty string
//        HdfString hdfString = new HdfString(bytes, StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII));
//
//        assertEquals("", hdfString.getValue());
//        assertArrayEquals(bytes, hdfString.getBytes());
//    }
//
//    @Test
//    public void testToStringOutput() {
//        byte[] bytes = new byte[]{65, 66, 67, 0}; // "ABC\0"
//        HdfString hdfString = new HdfString(bytes, StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII));
//
//        String toString = hdfString.toString();
//        assertTrue(toString.contains("ABC"));
//    }
//
//    @Test
//    public void testImmutability() {
//        byte[] bytes = new byte[]{65, 66, 67, 0}; // "ABC\0"
//        HdfString hdfString = new HdfString(bytes, StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII));
//
//        bytes[0] = 0; // Attempt to mutate original array
//        assertEquals("ABC", hdfString.getValue()); // Value should remain unchanged
//    }
//
//    @Test
//    public void testUtf8MultibyteCharacter() {
//        String multibyteString = "こんにちは"; // "Hello" in Japanese
//        HdfString hdfString = new HdfString(multibyteString);
//
//        assertEquals(multibyteString, hdfString.getValue());
//        assertArrayEquals((multibyteString).getBytes(StandardCharsets.UTF_8), hdfString.getBytes());
//    }
}
