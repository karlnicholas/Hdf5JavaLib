package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class HdfStringAppTest {
    private static final String[] ASCII_EXPECTED = {"label 1 ", "label 2 ", "label 3 ", "label 4 ", "label 5 ",
            "label 6 ", "label 7 ", "label 8 ", "label 9 ", "label 10"};
    private static final String[] UTF8_EXPECTED = {"ꦠꦤ꧀ 1", "ꦠꦤ꧀ 2", "ꦠꦤ꧀ 3", "ꦠꦤ꧀ 4", "ꦠꦤ꧀ 5",
            "ꦠꦤ꧀ 6", "ꦠꦤ꧀ 7", "ꦠꦤ꧀ 8", "ꦠꦤ꧀ 9", "ꦠꦤ꧀ 10"};
    private static final Set<String> ASCII_EXPECTED_SET = Set.of(ASCII_EXPECTED);
    private static final Set<String> UTF8_EXPECTED_SET = Set.of(UTF8_EXPECTED);
    // UTF-8 expected bytes padded with null terminator to match dataset size=12
    private static final byte[] UTF8_FIRST_ENTRY_BYTES = Arrays.copyOf("ꦠꦤ꧀ 1".getBytes(StandardCharsets.UTF_8), 12);

    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @Test
    void testAsciiDataset() throws IOException {
        Path filePath = getResourcePath("ascii_dataset.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("strings", channel, reader.getRootGroup());

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
                    .map(hdfString -> hdfString.getInstance(String.class))
                    .toArray(String[]::new);
            assertArrayEquals(ASCII_EXPECTED, hdfStringReadDataAsStrings);

            // HdfData
            TypedDataSource<HdfData> hdfDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] hdfDataReadData = hdfDataSource.readVector();
            String[] hdfDataReadDataAsStrings = Arrays.stream(hdfDataReadData)
                    .map(hdfData -> hdfData.getInstance(String.class))
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
    void testUtf8Dataset() throws IOException {
        Path filePath = getResourcePath("utf8_dataset.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("strings", channel, reader.getRootGroup());

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
                    .map(hdfString -> hdfString.getInstance(String.class))
                    .toArray(String[]::new);
            assertArrayEquals(UTF8_EXPECTED, hdfStringReadDataAsStrings);

            // HdfData
            TypedDataSource<HdfData> hdfDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] hdfDataReadData = hdfDataSource.readVector();
            String[] hdfDataReadDataAsStrings = Arrays.stream(hdfDataReadData)
                    .map(hdfData -> hdfData.getInstance(String.class))
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