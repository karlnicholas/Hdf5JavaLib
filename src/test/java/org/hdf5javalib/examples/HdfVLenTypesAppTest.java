package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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
    public void testVariableLengthTypes() throws IOException {
        Path filePath = getResourcePath("vlen_types_example.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();

            // vlen_double
            HdfDataSet doubleDs = reader.findDataset("vlen_double", channel, reader.getRootGroup());
            double[] expectedDoubles = {1.234, 5.678, 9.101};
            TypedDataSource<HdfVariableLength> doubleVlenSource = new TypedDataSource<>(channel, reader, doubleDs, HdfVariableLength.class);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleVlenSource.readScalar().getInstance(Object.class)), 1e-6);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleVlenSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)), 1e-6);
            TypedDataSource<String> doubleStringSource = new TypedDataSource<>(channel, reader, doubleDs, String.class);
            assertEquals("[1.234, 5.678, 9.101]", doubleStringSource.readScalar());
            assertEquals("[1.234, 5.678, 9.101]", doubleStringSource.streamScalar().findFirst().orElseThrow());
            TypedDataSource<Object> doubleObjectSource = new TypedDataSource<>(channel, reader, doubleDs, Object.class);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleObjectSource.readScalar()), 1e-6);
            assertArrayEquals(expectedDoubles, toDoubleArray((HdfData[]) doubleObjectSource.streamScalar().findFirst().orElseThrow()), 1e-6);

            // vlen_float
            HdfDataSet floatDs = reader.findDataset("vlen_float", channel, reader.getRootGroup());
            double[] expectedFloats = {1.100000023841858, 2.200000047683716, 3.299999952316284};
            TypedDataSource<HdfVariableLength> floatVlenSource = new TypedDataSource<>(channel, reader, floatDs, HdfVariableLength.class);
            assertArrayEquals(expectedFloats, toDoubleArray((HdfData[]) floatVlenSource.readScalar().getInstance(Object.class)), 1e-6);
            assertArrayEquals(expectedFloats, toDoubleArray((HdfData[]) floatVlenSource.streamScalar().findFirst().orElseThrow().getInstance(Object.class)), 1e-6);
            TypedDataSource<String> floatStringSource = new TypedDataSource<>(channel, reader, floatDs, String.class);
            assertEquals("[1.100000023841858, 2.200000047683716, 3.299999952316284]", floatStringSource.readScalar());
            assertEquals("[1.100000023841858, 2.200000047683716, 3.299999952316284]", floatStringSource.streamScalar().findFirst().orElseThrow());
            TypedDataSource<Object> floatObjectSource = new TypedDataSource<>(channel, reader, floatDs, Object.class);
            assertArrayEquals(expectedFloats, toDoubleArray((HdfData[]) floatObjectSource.readScalar()), 1e-6);
            assertArrayEquals(expectedFloats, toDoubleArray((HdfData[]) floatObjectSource.streamScalar().findFirst().orElseThrow()), 1e-6);

            // vlen_int
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

            // vlen_short
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

            // vlen_string
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
        }
    }

    private double[] toDoubleArray(HdfData[] data) {
        double[] result = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = data[i].getInstance(Double.class);
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
}