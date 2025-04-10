package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.FlattenedArrayUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class HdfFixedPointAppTest {
    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    // Scalar.h5 Tests
    @Test
    void testScalarReadScalar() throws IOException {
        Path filePath = getResourcePath("scalar.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("FixedPointValue", channel, reader.getRootGroup());
            try (TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigInteger.class)) {
                BigInteger scalar = dataSource.readScalar();
                assertEquals(BigInteger.valueOf(42), scalar);
            }
        }
    }

    @Test
    void testScalarStreamList() throws IOException {
        Path filePath = getResourcePath("scalar.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("FixedPointValue", channel, reader.getRootGroup());
            try (TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigInteger.class)) {
                List<BigInteger> streamed = dataSource.streamScalar().toList();
                assertEquals(List.of(BigInteger.valueOf(42)), streamed);
            }
        }
    }

    @Test
    void testScalarParallelStreamStats() throws IOException {
        Path filePath = getResourcePath("scalar.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("FixedPointValue", channel, reader.getRootGroup());
            try (TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigInteger.class)) {
                var stats = dataSource.parallelStreamScalar().collect(Collectors.summarizingInt(BigInteger::intValue));
                assertEquals(1, stats.getCount());
                assertEquals(42, stats.getSum());
                assertEquals(42, stats.getMin());
                assertEquals(42, stats.getMax());
            }
        }
    }

    // Vector.h5 Tests
    @Test
    void testVectorReadVectorValues() throws IOException {
        Path filePath = getResourcePath("vector.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("vector", channel, reader.getRootGroup());
            try (TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigInteger.class)) {
                BigInteger[] vector = dataSource.readVector();
                BigInteger[] expected = IntStream.rangeClosed(1, 100)
                        .mapToObj(BigInteger::valueOf)
                        .toArray(BigInteger[]::new);
                assertArrayEquals(expected, vector);
            }
        }
    }

    @Test
    void testVectorStreamStats() throws IOException {
        Path filePath = getResourcePath("vector.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("vector", channel, reader.getRootGroup());
            try (TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigInteger.class)) {
                var stats = dataSource.streamVector().collect(Collectors.summarizingInt(BigInteger::intValue));
                assertEquals(100, stats.getCount());
                assertEquals(5050, stats.getSum());
                assertEquals(1, stats.getMin());
                assertEquals(100, stats.getMax());
            }
        }
    }

    @Test
    void testVectorReducedMax() throws IOException {
        Path filePath = getResourcePath("vector.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("vector", channel, reader.getRootGroup());
            try (TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigInteger.class)) {
                BigInteger max = (BigInteger) FlattenedArrayUtils.reduceAlongAxis(dataSource.streamFlattened(), dataSource.getShape(), 0, BigInteger::max, BigInteger.class);
                assertEquals(BigInteger.valueOf(100), max);
            }
        }
    }

    // Weatherdata.h5 Tests
    @Test
    void testWeatherMatrixFirstRow() throws IOException {
        Path filePath = getResourcePath("weatherdata.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("weatherdata", channel, reader.getRootGroup());
            try (TypedDataSource<BigDecimal> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigDecimal.class)) {
                BigDecimal[][] matrix = dataSource.readMatrix();
                BigDecimal[] expectedFirstRow = Arrays.stream(new String[]{"20250216.00", "55.20", "30.40", "78.50", "40.40", "29.80", "48.70", "48.80", "48.30", "27.80", "0.60", "6.50", "9.78", "11.20", "0.05", "8.90", "34.60"})
                        .map(BigDecimal::new)
                        .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                        .toArray(BigDecimal[]::new);
                BigDecimal[] actualFirstRow = Arrays.stream(matrix[0])
                        .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                        .toArray(BigDecimal[]::new);
                assertArrayEquals(expectedFirstRow, actualFirstRow);
            }
        }
    }

    @Test
    void testWeatherStreamMatrixShape() throws IOException {
        Path filePath = getResourcePath("weatherdata.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("weatherdata", channel, reader.getRootGroup());
            try (TypedDataSource<BigDecimal> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigDecimal.class)) {
                List<BigDecimal[]> streamed = dataSource.streamMatrix().toList();
                assertEquals(4, streamed.size());
                assertEquals(17, streamed.get(0).length);
            }
        }
    }

    @Test
    void testWeatherReducedMax() throws IOException {
        Path filePath = getResourcePath("weatherdata.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("weatherdata", channel, reader.getRootGroup());
            try (TypedDataSource<BigDecimal> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigDecimal.class)) {
                BigDecimal[] reduced = (BigDecimal[]) FlattenedArrayUtils.reduceAlongAxis(dataSource.streamFlattened(), dataSource.getShape(), 0, BigDecimal::max, BigDecimal.class);
                BigDecimal[] expectedMax = Arrays.stream(new String[]{"20250219.00", "55.20", "33.30", "79.40", "40.40", "30.15", "48.70", "48.80", "48.30", "27.80", "0.60", "6.54", "9.82", "17.80", "0.07", "8.90", "34.60"})
                        .map(BigDecimal::new)
                        .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                        .toArray(BigDecimal[]::new);
                BigDecimal[] actualReduced = Arrays.stream(reduced)
                        .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                        .toArray(BigDecimal[]::new);
                assertArrayEquals(expectedMax, actualReduced);
            }
        }
    }

    @Test
    void testWeatherParallelStreamConsistency() throws IOException {
        Path filePath = getResourcePath("weatherdata.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("weatherdata", channel, reader.getRootGroup());
            try (TypedDataSource<BigDecimal> dataSource = new TypedDataSource<>(dataSet, channel, reader, BigDecimal.class)) {
                List<BigDecimal[]> parallelStreamed = dataSource.parallelStreamMatrix().toList();
                assertEquals(4, parallelStreamed.size());
                BigDecimal[] expectedFirstRow = Arrays.stream(new String[]{"20250216.00", "55.20", "30.40", "78.50", "40.40", "29.80", "48.70", "48.80", "48.30", "27.80", "0.60", "6.50", "9.78", "11.20", "0.05", "8.90", "34.60"})
                        .map(BigDecimal::new)
                        .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                        .toArray(BigDecimal[]::new);
                BigDecimal[] actualFirstRow = Arrays.stream(parallelStreamed.get(0))
                        .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                        .toArray(BigDecimal[]::new);
                assertArrayEquals(expectedFirstRow, actualFirstRow);
            }
        }
    }

    // Tictactoe_4d_state.h5 Tests
    @Test
    void testTictactoeShape() throws IOException {
        Path filePath = getResourcePath("tictactoe_4d_state.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("game", channel, reader.getRootGroup());
            try (TypedDataSource<Integer> dataSource = new TypedDataSource<>(dataSet, channel, reader, Integer.class)) {
                int[] shape = dataSource.getShape();
                assertArrayEquals(new int[]{3, 3, 3, 5}, shape);
            }
        }
    }

    @Test
    void testTictactoeStep0FirstValue() throws IOException {
        Path filePath = getResourcePath("tictactoe_4d_state.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("game", channel, reader.getRootGroup());
            try (TypedDataSource<Integer> dataSource = new TypedDataSource<>(dataSet, channel, reader, Integer.class)) {
                int[][] sliceStep0 = {{}, {}, {}, {0}};
                Integer[][][] step0 = (Integer[][][]) FlattenedArrayUtils.sliceStream(dataSource.streamFlattened(), dataSource.getShape(), sliceStep0, Integer.class);
                assertEquals(1, step0[0][0][0]);
            }
        }
    }

    @Test
    void testTictactoeNonZeroPiecesCount() throws IOException {
        Path filePath = getResourcePath("tictactoe_4d_state.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("game", channel, reader.getRootGroup());
            try (TypedDataSource<Integer> dataSource = new TypedDataSource<>(dataSet, channel, reader, Integer.class)) {
                List<FlattenedArrayUtils.MatchingEntry<Integer>> pieces = FlattenedArrayUtils.filterToCoordinateList(dataSource.streamFlattened(), dataSource.getShape(), i -> i != 0);
                assertEquals(15, pieces.size());
            }
        }
    }

    @Test
    void testTictactoePiecesFirstAndLast() throws IOException {
        Path filePath = getResourcePath("tictactoe_4d_state.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("game", channel, reader.getRootGroup());
            try (TypedDataSource<Integer> dataSource = new TypedDataSource<>(dataSet, channel, reader, Integer.class)) {
                List<FlattenedArrayUtils.MatchingEntry<Integer>> pieces = FlattenedArrayUtils.filterToCoordinateList(dataSource.streamFlattened(), dataSource.getShape(), i -> i != 0);
                pieces.sort((a, b) -> Arrays.compare(a.coordinates, b.coordinates));
                FlattenedArrayUtils.MatchingEntry<Integer> first = pieces.get(0);
                assertArrayEquals(new int[]{0, 0, 0, 0}, first.coordinates);
                assertEquals(1, first.value);
                FlattenedArrayUtils.MatchingEntry<Integer> last = pieces.get(pieces.size() - 1);
                assertArrayEquals(new int[]{1, 1, 0, 4}, last.coordinates);
                assertEquals(2, last.value);
            }
        }
    }
}