package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HdfVLenTypesAppTest {

    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @Test
    public void testVlenDouble() throws IOException {
        Path filePath = getResourcePath("vlen_types_example.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet doubleDs = reader.findDataset("vlen_double", channel, reader.getRootGroup());
            double[] expectedDoubles = {1.234, 5.678, 9.101};
            byte[][] expectedDoubleBytes = {
                    ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putDouble(1.234).array(),
                    ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putDouble(5.678).array(),
                    ByteBuffer.allocate(8).order(java.nio.ByteOrder.LITTLE_ENDIAN).putDouble(9.101).array()
            };
            TypedDataSource<HdfVariableLength> doubleVlenSource = new TypedDataSource<>(channel, reader, doubleDs, HdfVariableLength.class);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleVlenSource.readScalar().getInstance(Object.class)), 1e-6);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleVlenSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)), 1e-6);
            TypedDataSource<String> doubleStringSource = new TypedDataSource<>(channel, reader, doubleDs, String.class);
            assertEquals("[1.234, 5.678, 9.101]", doubleStringSource.readScalar());
            assertEquals("[1.234, 5.678, 9.101]", doubleStringSource.streamScalar().findFirst().orElseThrow());
            TypedDataSource<Object> doubleObjectSource = new TypedDataSource<>(channel, reader, doubleDs, Object.class);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleObjectSource.readScalar()), 1e-6);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleObjectSource.streamScalar().findFirst().orElseThrow()), 1e-6);
            TypedDataSource<HdfData> doubleHdfDataSource = new TypedDataSource<>(channel, reader, doubleDs, HdfData.class);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleHdfDataSource.readScalar().getInstance(Object.class)), 1e-6);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleHdfDataSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)), 1e-6);
            TypedDataSource<byte[][]> doubleByteSource = new TypedDataSource<>(channel, reader, doubleDs, byte[][].class);
            assertArrayEquals(expectedDoubleBytes, doubleByteSource.readScalar());
            assertArrayEquals(expectedDoubleBytes, doubleByteSource.streamScalar().findFirst().orElseThrow());
        }
    }

    @Test
    public void testVlenFloat() throws IOException {
        Path filePath = getResourcePath("vlen_types_example.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet floatDs = reader.findDataset("vlen_float", channel, reader.getRootGroup());
            float[] expectedFloats = {1.1f, 2.2f, 3.3f};
            byte[][] expectedFloatBytes = {
                    ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putFloat(1.1f).array(),
                    ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putFloat(2.2f).array(),
                    ByteBuffer.allocate(4).order(java.nio.ByteOrder.LITTLE_ENDIAN).putFloat(3.3f).array()
            };
            TypedDataSource<HdfVariableLength> floatVlenSource = new TypedDataSource<>(channel, reader, floatDs, HdfVariableLength.class);
            assertArrayEquals(expectedFloats, toFloatArray((HdfData[]) floatVlenSource.readScalar().getInstance(Object.class)), 1e-6f);
            assertArrayEquals(expectedFloats, toFloatArray((HdfData[]) floatVlenSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)), 1e-6f);
            TypedDataSource<String> floatStringSource = new TypedDataSource<>(channel, reader, floatDs, String.class);
            assertEquals("[1.1, 2.2, 3.3]", floatStringSource.readScalar());
            assertEquals("[1.1, 2.2, 3.3]", floatStringSource.streamScalar().findFirst().orElseThrow());
            TypedDataSource<Object> floatObjectSource = new TypedDataSource<>(channel, reader, floatDs, Object.class);
            assertArrayEquals(expectedFloats, toFloatArray((HdfData[]) floatObjectSource.readScalar()), 1e-6f);
            assertArrayEquals(expectedFloats, toFloatArray((HdfData[]) floatObjectSource.streamScalar().findFirst().orElseThrow()), 1e-6f);
            TypedDataSource<HdfData> floatHdfDataSource = new TypedDataSource<>(channel, reader, floatDs, HdfData.class);
            assertArrayEquals(expectedFloats, toFloatArray((HdfData[]) floatHdfDataSource.readScalar().getInstance(Object.class)), 1e-6f);
            assertArrayEquals(expectedFloats, toFloatArray((HdfData[]) floatHdfDataSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)), 1e-6f);
            TypedDataSource<byte[][]> floatByteSource = new TypedDataSource<>(channel, reader, floatDs, byte[][].class);
            assertArrayEquals(expectedFloatBytes, floatByteSource.readScalar());
            assertArrayEquals(expectedFloatBytes, floatByteSource.streamScalar().findFirst().orElseThrow());
        }
    }

    @Test
    public void testVlenInt() throws IOException {
        Path filePath = getResourcePath("vlen_types_example.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet intDs = reader.findDataset("vlen_int", channel, reader.getRootGroup());
            int[] expectedInts = {1, 2, 3, 4, 5};
            TypedDataSource<HdfVariableLength> intVlenSource = new TypedDataSource<>(channel, reader, intDs, HdfVariableLength.class);
            assertArrayEquals(expectedInts, toIntArray((HdfData[]) intVlenSource.readScalar().getInstance(Object.class)));
            assertArrayEquals(expectedInts, toIntArray((HdfData[]) intVlenSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)));
            TypedDataSource<String> intStringSource = new TypedDataSource<>(channel, reader, intDs, String.class);
            assertEquals("[1, 2, 3, 4, 5]", intStringSource.readScalar());
            assertEquals("[1, 2, 3, 4, 5]", intStringSource.streamScalar().findFirst().orElseThrow());
            TypedDataSource<Object> intObjectSource = new TypedDataSource<>(channel, reader, intDs, Object.class);
            assertArrayEquals(expectedInts, toIntArray((HdfData[]) intObjectSource.readScalar()));
            assertArrayEquals(expectedInts, toIntArray((HdfData[]) intObjectSource.streamScalar().findFirst().orElseThrow()));
            TypedDataSource<HdfData> intHdfDataSource = new TypedDataSource<>(channel, reader, intDs, HdfData.class);
            assertArrayEquals(expectedInts, toIntArray((HdfData[]) intHdfDataSource.readScalar().getInstance(Object.class)));
            assertArrayEquals(expectedInts, toIntArray((HdfData[]) intHdfDataSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)));
        }
    }

    @Test
    public void testVlenShort() throws IOException {
        Path filePath = getResourcePath("vlen_types_example.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet shortDs = reader.findDataset("vlen_short", channel, reader.getRootGroup());
            short[] expectedShorts = {10, 20, 30};
            TypedDataSource<HdfVariableLength> shortVlenSource = new TypedDataSource<>(channel, reader, shortDs, HdfVariableLength.class);
            assertArrayEquals(expectedShorts, toShortArray((HdfData[]) shortVlenSource.readScalar().getInstance(Object.class)));
            assertArrayEquals(expectedShorts, toShortArray((HdfData[]) shortVlenSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)));
            TypedDataSource<String> shortStringSource = new TypedDataSource<>(channel, reader, shortDs, String.class);
            assertEquals("[10, 20, 30]", shortStringSource.readScalar());
            assertEquals("[10, 20, 30]", shortStringSource.streamScalar().findFirst().orElseThrow());
            TypedDataSource<Object> shortObjectSource = new TypedDataSource<>(channel, reader, shortDs, Object.class);
            assertArrayEquals(expectedShorts, toShortArray((HdfData[]) shortObjectSource.readScalar()));
            assertArrayEquals(expectedShorts, toShortArray((HdfData[]) shortObjectSource.streamScalar().findFirst().orElseThrow()));
            TypedDataSource<HdfData> shortHdfDataSource = new TypedDataSource<>(channel, reader, shortDs, HdfData.class);
            assertArrayEquals(expectedShorts, toShortArray((HdfData[]) shortHdfDataSource.readScalar().getInstance(Object.class)));
            assertArrayEquals(expectedShorts, toShortArray((HdfData[]) shortHdfDataSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)));
        }
    }

    @Test
    public void testVlenString() throws IOException {
        Path filePath = getResourcePath("vlen_types_example.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet stringDs = reader.findDataset("vlen_string", channel, reader.getRootGroup());
            String expectedString = "Hello, Variable Length String!";
            int[] expectedBytes = {72, 101, 108, 108, 111, 44, 32, 86, 97, 114, 105, 97, 98, 108, 101,
                    32, 76, 101, 110, 103, 116, 104, 32, 83, 116, 114, 105, 110, 103, 33};
            TypedDataSource<HdfVariableLength> stringVlenSource = new TypedDataSource<>(channel, reader, stringDs, HdfVariableLength.class);
            assertEquals(expectedString, stringVlenSource.readScalar().getInstance(String.class));
            assertEquals(expectedString, stringVlenSource.streamScalar().findFirst().orElseThrow().getInstance(String.class));
            TypedDataSource<String> stringStringSource = new TypedDataSource<>(channel, reader, stringDs, String.class);
            assertEquals(expectedString, stringStringSource.readScalar());
            assertEquals(expectedString, stringStringSource.streamScalar().findFirst().orElseThrow());
            TypedDataSource<Object> stringObjectSource = new TypedDataSource<>(channel, reader, stringDs, Object.class);
            assertArrayEquals(expectedBytes, toIntArray((HdfData[]) stringObjectSource.readScalar()));
            assertArrayEquals(expectedBytes, toIntArray((HdfData[]) stringObjectSource.streamScalar().findFirst().orElseThrow()));
            TypedDataSource<HdfData> stringHdfDataSource = new TypedDataSource<>(channel, reader, stringDs, HdfData.class);
            assertEquals(expectedString, stringHdfDataSource.readScalar().getInstance(String.class));
            assertEquals(expectedString, stringHdfDataSource.streamScalar().findFirst().orElseThrow().getInstance(String.class));
            TypedDataSource<byte[][]> stringByteSource = new TypedDataSource<>(channel, reader, stringDs, byte[][].class);
            byte[][] stringBytes = stringByteSource.readScalar();
            byte[] flattenedBytes = flattenByteArrayArray(stringBytes);
            assertEquals(expectedString, new String(flattenedBytes, StandardCharsets.US_ASCII));
            byte[][] streamStringBytes = stringByteSource.streamScalar().findFirst().orElseThrow();
            byte[] flattenedStreamBytes = flattenByteArrayArray(streamStringBytes);
            assertEquals(expectedString, new String(flattenedStreamBytes, StandardCharsets.US_ASCII));
        }
    }

    private double[] toDoubleArray(HdfData[] data) {
        double[] result = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = data[i].getInstance(Double.class);
        }
        return result;
    }

    private float[] toFloatArray(HdfData[] data) {
        float[] result = new float[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = data[i].getInstance(Float.class);
        }
        return result;
    }

    private int[] toIntArray(HdfData[] data) {
        int[] result = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = data[i].getInstance(Integer.class);
        }
        return result;
    }

    private short[] toShortArray(HdfData[] data) {
        short[] result = new short[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = data[i].getInstance(Short.class);
        }
        return result;
    }

    private byte[] flattenByteArrayArray(byte[][] byteArrayArray) {
        int totalLength = 0;
        for (byte[] bytes : byteArrayArray) {
            totalLength += bytes.length;
        }
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] bytes : byteArrayArray) {
            System.arraycopy(bytes, 0, result, offset, bytes.length);
            offset += bytes.length;
        }
        return result;
    }
}