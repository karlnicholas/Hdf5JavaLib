package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfFixedPoint;
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

    @Test
    void testScalarH5() throws IOException {
        Path filePath = getResourcePath("scalar.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("FixedPointValue", channel, reader.getRootGroup());

            // BigInteger
            TypedDataSource<BigInteger> bigIntSource = new TypedDataSource<>(channel, reader, dataSet, BigInteger.class);
            BigInteger scalar = bigIntSource.readScalar();
            assertEquals(BigInteger.valueOf(42), scalar);
            List<BigInteger> streamed = bigIntSource.streamScalar().toList();
            assertEquals(List.of(BigInteger.valueOf(42)), streamed);
            var stats = bigIntSource.parallelStreamScalar().collect(Collectors.summarizingInt(BigInteger::intValue));
            assertEquals(1, stats.getCount());
            assertEquals(42, stats.getSum());
            assertEquals(42, stats.getMin());
            assertEquals(42, stats.getMax());

            // HdfFixedPoint
            TypedDataSource<HdfFixedPoint> fixedPointSource = new TypedDataSource<>(channel, reader, dataSet, HdfFixedPoint.class);
            List<HdfFixedPoint> fixedPointStreamed = fixedPointSource.streamScalar().toList();
            assertEquals(1, fixedPointStreamed.size());
            assertEquals(42, fixedPointStreamed.get(0).getInstance(Long.class).intValue());

            // String
            TypedDataSource<String> stringSource = new TypedDataSource<>(channel, reader, dataSet, String.class);
            List<String> stringStreamed = stringSource.streamScalar().toList();
            assertEquals(List.of("42"), stringStreamed);

            // BigDecimal
            TypedDataSource<BigDecimal> bigDecimalSource = new TypedDataSource<>(channel, reader, dataSet, BigDecimal.class);
            List<BigDecimal> bigDecimalStreamed = bigDecimalSource.streamScalar().toList();
            assertEquals(List.of(BigDecimal.valueOf(42)), bigDecimalStreamed);
        }
    }

    @Test
    void testVectorH5() throws IOException {
        Path filePath = getResourcePath("vector.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("vector", channel, reader.getRootGroup());
            TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(channel, reader, dataSet, BigInteger.class);

            BigInteger[] vector = dataSource.readVector();
            BigInteger[] expected = IntStream.rangeClosed(1, 1000)
                    .mapToObj(BigInteger::valueOf)
                    .toArray(BigInteger[]::new);
            assertArrayEquals(expected, vector);

            var stats = dataSource.streamVector().collect(Collectors.summarizingInt(BigInteger::intValue));
            assertEquals(1000, stats.getCount());
            assertEquals(500500, stats.getSum());
            assertEquals(1, stats.getMin());
            assertEquals(1000, stats.getMax());

            BigInteger[] flattened = dataSource.readFlattened();
            assertEquals(1000, flattened.length);
            assertArrayEquals(expected, flattened);

            BigInteger max = (BigInteger) FlattenedArrayUtils.reduceAlongAxis(
                    dataSource.parallelStreamFlattened(), dataSource.getShape(), 0, BigInteger::max, BigInteger.class);
            assertEquals(BigInteger.valueOf(1000), max);
        }
    }

    @Test
    void testWeatherdataH5() throws IOException {
        Path filePath = getResourcePath("weatherdata.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("weatherdata", channel, reader.getRootGroup());
            TypedDataSource<BigDecimal> dataSource = new TypedDataSource<>(channel, reader, dataSet, BigDecimal.class);

            BigDecimal[][] matrix = dataSource.readMatrix();
            BigDecimal[] expectedFirstRow = Arrays.stream(new String[]{"20250216.00", "55.20", "30.40", "78.50", "40.40", "29.80", "48.70", "48.80", "48.30", "27.80", "0.60", "6.50", "9.78", "11.20", "0.05", "8.90", "34.60"})
                    .map(BigDecimal::new)
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            BigDecimal[] actualFirstRow = Arrays.stream(matrix[0])
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            assertArrayEquals(expectedFirstRow, actualFirstRow);

            List<BigDecimal[]> streamed = dataSource.streamMatrix().toList();
            assertEquals(4, streamed.size());
            assertEquals(17, streamed.get(0).length);

            BigDecimal[] flattened = dataSource.readFlattened();
            assertEquals(4 * 17, flattened.length);
            assertEquals(expectedFirstRow[0], flattened[0].setScale(2, RoundingMode.HALF_UP));

            BigDecimal[] reduced = (BigDecimal[]) FlattenedArrayUtils.reduceAlongAxis(dataSource.streamFlattened(), dataSource.getShape(), 0, BigDecimal::max, BigDecimal.class);
            BigDecimal[] expectedMax = Arrays.stream(new String[]{"20250219.00", "55.20", "33.30", "79.40", "40.40", "30.15", "48.70", "48.80", "48.30", "27.80", "0.60", "6.54", "9.82", "17.80", "0.07", "8.90", "34.60"})
                    .map(BigDecimal::new)
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            BigDecimal[] actualReduced = Arrays.stream(reduced)
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            assertArrayEquals(expectedMax, actualReduced);

            List<BigDecimal[]> parallelStreamed = dataSource.parallelStreamMatrix().toList();
            assertEquals(4, parallelStreamed.size());
            BigDecimal[] expectedFirstRowParallel = Arrays.stream(new String[]{"20250216.00", "55.20", "30.40", "78.50", "40.40", "29.80", "48.70", "48.80", "48.30", "27.80", "0.60", "6.50", "9.78", "11.20", "0.05", "8.90", "34.60"})
                    .map(BigDecimal::new)
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            BigDecimal[] actualFirstRowParallel = Arrays.stream(parallelStreamed.get(0))
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            assertArrayEquals(expectedFirstRowParallel, actualFirstRowParallel);
        }
    }

    @Test
    void testTictactoe4dStateH5() throws IOException {
        Path filePath = getResourcePath("tictactoe_4d_state.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("game", channel, reader.getRootGroup());
            TypedDataSource<Integer> dataSource = new TypedDataSource<>(channel, reader, dataSet, Integer.class);

            int[] shape = dataSource.getShape();
            assertArrayEquals(new int[]{3, 3, 3, 5}, shape);

            Integer[] flattened = dataSource.readFlattened();
            assertEquals(3 * 3 * 3 * 5, flattened.length);

            int[][] sliceStep0 = {{}, {}, {}, {0}};
            Integer[][][] step0 = (Integer[][][]) FlattenedArrayUtils.sliceStream(dataSource.streamFlattened(), dataSource.getShape(), sliceStep0, Integer.class);
            assertEquals(1, step0[0][0][0]);

            List<FlattenedArrayUtils.MatchingEntry<Integer>> pieces = FlattenedArrayUtils.filterToCoordinateList(dataSource.streamFlattened(), dataSource.getShape(), i -> i != 0);
            assertEquals(15, pieces.size());

            pieces.sort((a, b) -> Arrays.compare(a.coordinates, b.coordinates));
            FlattenedArrayUtils.MatchingEntry<Integer> firstPiece = pieces.get(0);
            assertArrayEquals(new int[]{0, 0, 0, 0}, firstPiece.coordinates);
            assertEquals(1, firstPiece.value);
            FlattenedArrayUtils.MatchingEntry<Integer> lastPiece = pieces.get(pieces.size() - 1);
            assertArrayEquals(new int[]{1, 1, 0, 4}, lastPiece.coordinates);
            assertEquals(2, lastPiece.value);
        }
    }
}