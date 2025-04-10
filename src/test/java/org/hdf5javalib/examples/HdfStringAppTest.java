package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class HdfStringAppTest {
    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @Test
    void testAsciiDatasetH5() throws IOException {
        Path filePath = getResourcePath("ascii_dataset.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("strings", channel, reader.getRootGroup());
            TypedDataSource<String> dataSource = new TypedDataSource<>(channel, reader, dataSet, String.class);

            // testAsciiDatasetRead
            String[] readData = dataSource.readVector();
            String[] expected = {"label 1 ", "label 2 ", "label 3 ", "label 4 ", "label 5 ",
                    "label 6 ", "label 7 ", "label 8 ", "label 9 ", "label 10"};
            assertArrayEquals(expected, readData);

            // testAsciiDatasetStream
            List<String> streamedData = dataSource.streamVector().collect(Collectors.toList());
            List<String> expectedStream = Arrays.asList("label 1 ", "label 2 ", "label 3 ", "label 4 ", "label 5 ",
                    "label 6 ", "label 7 ", "label 8 ", "label 9 ", "label 10");
            assertEquals(expectedStream, streamedData);
        }
    }

    @Test
    void testUtf8DatasetH5() throws IOException {
        Path filePath = getResourcePath("utf8_dataset.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("strings", channel, reader.getRootGroup());
            TypedDataSource<String> dataSource = new TypedDataSource<>(channel, reader, dataSet, String.class);

            // testUtf8DatasetRead
            String[] readData = dataSource.readVector();
            String[] expected = {"ꦠꦤ꧀ 1", "ꦠꦤ꧀ 2", "ꦠꦤ꧀ 3", "ꦠꦤ꧀ 4", "ꦠꦤ꧀ 5",
                    "ꦠꦤ꧀ 6", "ꦠꦤ꧀ 7", "ꦠꦤ꧀ 8", "ꦠꦤ꧀ 9", "ꦠꦤ꧀ 10"};
            assertArrayEquals(expected, readData);

            // testUtf8DatasetStream
            List<String> streamedData = dataSource.streamVector().collect(Collectors.toList());
            List<String> expectedStream = Arrays.asList("ꦠꦤ꧀ 1", "ꦠꦤ꧀ 2", "ꦠꦤ꧀ 3", "ꦠꦤ꧀ 4", "ꦠꦤ꧀ 5",
                    "ꦠꦤ꧀ 6", "ꦠꦤ꧀ 7", "ꦠꦤ꧀ 8", "ꦠꦤ꧀ 9", "ꦠꦤ꧀ 10");
            assertEquals(expectedStream, streamedData);
        }
    }

    @Test
    void testStringAsciiAllH5() throws IOException {
        Path filePath = getResourcePath("string_ascii_all.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("strings", channel, reader.getRootGroup());
            TypedDataSource<String> dataSource = new TypedDataSource<>(channel, reader, dataSet, String.class);

            // testStringAsciiAllRead
            String[] readData = dataSource.readVector();
            String[] expected = {"label 1 ", "label 2 ", "label 3 ", "label 4 ", "label 5 ",
                    "label 6 ", "label 7 ", "label 8 ", "label 9 ", "label 10"};
            assertArrayEquals(expected, readData);

            // testStringAsciiAllStream
            List<String> streamedData = dataSource.streamVector().collect(Collectors.toList());
            List<String> expectedStream = Arrays.asList("label 1 ", "label 2 ", "label 3 ", "label 4 ", "label 5 ",
                    "label 6 ", "label 7 ", "label 8 ", "label 9 ", "label 10");
            assertEquals(expectedStream, streamedData);
        }
    }

    @Test
    void testStringUtf8EachH5() throws IOException {
        Path filePath = getResourcePath("string_utf8_each.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("strings", channel, reader.getRootGroup());
            TypedDataSource<String> dataSource = new TypedDataSource<>(channel, reader, dataSet, String.class);

            // testStringUtf8EachRead
            String[] readData = dataSource.readVector();
            String[] expected = {"ꦠꦤ꧀ 1", "ꦠꦤ꧀ 2", "ꦠꦤ꧀ 3", "ꦠꦤ꧀ 4", "ꦠꦤ꧀ 5",
                    "ꦠꦤ꧀ 6", "ꦠꦤ꧀ 7", "ꦠꦤ꧀ 8", "ꦠꦤ꧀ 9", "ꦠꦤ꧀ 10"};
            assertArrayEquals(expected, readData);

            // testStringUtf8EachStream
            List<String> streamedData = dataSource.streamVector().collect(Collectors.toList());
            List<String> expectedStream = Arrays.asList("ꦠꦤ꧀ 1", "ꦠꦤ꧀ 2", "ꦠꦤ꧀ 3", "ꦠꦤ꧀ 4", "ꦠꦤ꧀ 5",
                    "ꦠꦤ꧀ 6", "ꦠꦤ꧀ 7", "ꦠꦤ꧀ 8", "ꦠꦤ꧀ 9", "ꦠꦤ꧀ 10");
            assertEquals(expectedStream, streamedData);
        }
    }
}