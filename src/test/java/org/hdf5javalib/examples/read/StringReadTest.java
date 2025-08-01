package org.hdf5javalib.examples.read;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringReadTest {
    private static final String[] ASCII_EXPECTED = {"label 1 ", "label 2 ", "label 3 ", "label 4 ", "label 5 ",
            "label 6 ", "label 7 ", "label 8 ", "label 9 ", "label 10"};
    private static final String[] UTF8_EXPECTED = {"ꦠꦤ꧀ 1", "ꦠꦤ꧀ 2", "ꦠꦤ꧀ 3", "ꦠꦤ꧀ 4", "ꦠꦤ꧀ 5",
            "ꦠꦤ꧀ 6", "ꦠꦤ꧀ 7", "ꦠꦤ꧀ 8", "ꦠꦤ꧀ 9", "ꦠꦤ꧀ 10"};
    private static final Set<String> ASCII_EXPECTED_SET = Set.of(ASCII_EXPECTED);
    private static final Set<String> UTF8_EXPECTED_SET = Set.of(UTF8_EXPECTED);
    // UTF-8 expected bytes padded with null terminator to match dataset size=12
    private static final byte[] UTF8_FIRST_ENTRY_BYTES = Arrays.copyOf("ꦠꦤ꧀ 1".getBytes(StandardCharsets.UTF_8), 12);

    @Test
    void testAsciiDataset() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("ascii_dataset.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("strings").orElseThrow();

            // String
            TypedDataSource<String> stringDataSource = new TypedDataSource<>(channel, reader, dataSet, String.class);
            String[] stringReadData = stringDataSource.readVector();
            assertArrayEquals(ASCII_EXPECTED, stringReadData);
            Set<String> stringStreamedData = stringDataSource.streamVector().collect(Collectors.toSet());
            assertEquals(ASCII_EXPECTED_SET, stringStreamedData);
            Set<String> stringParallelStreamedData = stringDataSource.parallelStreamVector().collect(Collectors.toSet());
            assertEquals(ASCII_EXPECTED_SET, stringParallelStreamedData);

            // HdfString
            TypedDataSource<HdfString> hdfStringDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfString.class);
            HdfString[] hdfStringReadData = hdfStringDataSource.readVector();
            String[] hdfStringReadDataAsStrings = Arrays.stream(hdfStringReadData)
                    .map(hdfString -> {
                        try {
                            return hdfString.getInstance(String.class);
                        } catch (InvocationTargetException e) {
                            throw new RuntimeException(e);
                        } catch (InstantiationException e) {
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(String[]::new);
            assertArrayEquals(ASCII_EXPECTED, hdfStringReadDataAsStrings);

            // HdfData
            TypedDataSource<HdfData> hdfDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] hdfDataReadData = hdfDataSource.readVector();
            String[] hdfDataReadDataAsStrings = Arrays.stream(hdfDataReadData)
                    .map(hdfData -> {
                        try {
                            return hdfData.getInstance(String.class);
                        } catch (InvocationTargetException e) {
                            throw new RuntimeException(e);
                        } catch (InstantiationException e) {
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(String[]::new);
            assertArrayEquals(ASCII_EXPECTED, hdfDataReadDataAsStrings);

            // byte[]
            TypedDataSource<byte[]> byteDataSource = new TypedDataSource<>(channel, reader, dataSet, byte[].class);
            byte[][] byteReadData = byteDataSource.readVector();
            assertEquals(10, byteReadData.length);
            assertArrayEquals("label 1 ".getBytes(StandardCharsets.US_ASCII), byteReadData[0]);
        }
    }

    @Test
    void testUtf8Dataset() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("utf8_dataset.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("strings").orElseThrow();

            // String
            TypedDataSource<String> stringDataSource = new TypedDataSource<>(channel, reader, dataSet, String.class);
            String[] stringReadData = stringDataSource.readVector();
            assertArrayEquals(UTF8_EXPECTED, stringReadData);
            Set<String> stringStreamedData = stringDataSource.streamVector().collect(Collectors.toSet());
            assertEquals(UTF8_EXPECTED_SET, stringStreamedData);
            Set<String> stringParallelStreamedData = stringDataSource.parallelStreamVector().collect(Collectors.toSet());
            assertEquals(UTF8_EXPECTED_SET, stringParallelStreamedData);

            // HdfString
            TypedDataSource<HdfString> hdfStringDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfString.class);
            HdfString[] hdfStringReadData = hdfStringDataSource.readVector();
            String[] hdfStringReadDataAsStrings = Arrays.stream(hdfStringReadData)
                    .map(hdfString -> {
                        try {
                            return hdfString.getInstance(String.class);
                        } catch (InvocationTargetException e) {
                            throw new RuntimeException(e);
                        } catch (InstantiationException e) {
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(String[]::new);
            assertArrayEquals(UTF8_EXPECTED, hdfStringReadDataAsStrings);

            // HdfData
            TypedDataSource<HdfData> hdfDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] hdfDataReadData = hdfDataSource.readVector();
            String[] hdfDataReadDataAsStrings = Arrays.stream(hdfDataReadData)
                    .map(hdfData -> {
                        try {
                            return hdfData.getInstance(String.class);
                        } catch (InvocationTargetException e) {
                            throw new RuntimeException(e);
                        } catch (InstantiationException e) {
                            throw new RuntimeException(e);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(String[]::new);
            assertArrayEquals(UTF8_EXPECTED, hdfDataReadDataAsStrings);

            // byte[]
            TypedDataSource<byte[]> byteDataSource = new TypedDataSource<>(channel, reader, dataSet, byte[].class);
            byte[][] byteReadData = byteDataSource.readVector();
            assertEquals(10, byteReadData.length);
            assertArrayEquals(UTF8_FIRST_ENTRY_BYTES, byteReadData[0]);
        }
    }
}