package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.FlattenedArrayUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class HdfFixedPointAppTest {
    private static final BigInteger[] VECTOR_EXPECTED = IntStream.rangeClosed(1, 1000)
            .mapToObj(BigInteger::valueOf)
            .toArray(BigInteger[]::new);
    private static final BigDecimal[] WEATHER_FIRST_ROW_EXPECTED = Arrays.stream(new String[]{"20250216.00", "55.20", "30.40", "78.50", "40.40", "29.80", "48.70", "48.80", "48.30", "27.80", "0.60", "6.50", "9.78", "11.20", "0.05", "8.90", "34.60"})
            .map(BigDecimal::new)
            .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
            .toArray(BigDecimal[]::new);
    private static final BigDecimal[] WEATHER_MAX_EXPECTED = Arrays.stream(new String[]{"20250219.00", "55.20", "33.30", "79.40", "40.40", "30.15", "48.70", "48.80", "48.30", "27.80", "0.60", "6.54", "9.82", "17.80", "0.07", "8.90", "34.60"})
            .map(BigDecimal::new)
            .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
            .toArray(BigDecimal[]::new);
    private static final int[] TICTACTOE_SHAPE_EXPECTED = {3, 3, 3, 5};
    private static final int[] TICTACTOE_FIRST_PIECE_COORDS = {0, 0, 0, 0};
    private static final int[] TICTACTOE_LAST_PIECE_COORDS = {1, 1, 0, 4};

    private Path getResourcePath(String fileName) {
        String resourcePath = Objects.requireNonNull(getClass().getClassLoader().getResource(fileName)).getPath();
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

            // "byte" dataset (1 byte, value 42)
            HdfDataSet byteDataSet = reader.findDataset("byte", channel, reader.getRootGroup());
            testConverters(byteDataSet, channel, reader, (byte) 42, 1);

            // "short" dataset (2 bytes, value 42)
            HdfDataSet shortDataSet = reader.findDataset("short", channel, reader.getRootGroup());
            testConverters(shortDataSet, channel, reader, (short) 42, 2);

            // "integer" dataset (4 bytes, value 42)
            HdfDataSet intDataSet = reader.findDataset("integer", channel, reader.getRootGroup());
            testConverters(intDataSet, channel, reader, 42, 4);

            // "long" dataset (8 bytes, value 42)
            HdfDataSet longDataSet = reader.findDataset("long", channel, reader.getRootGroup());
            testConverters(longDataSet, channel, reader, 42L, 8);
        }
    }

    private void testConverters(HdfDataSet dataSet, SeekableByteChannel channel, HdfFileReader reader, Number expected,
                                int byteSize) throws IOException {
        // BigInteger (always works)
        TypedDataSource<BigInteger> bigIntSource = new TypedDataSource<>(channel, reader, dataSet, BigInteger.class);
        assertEquals(BigInteger.valueOf(expected.longValue()), bigIntSource.readScalar());

        // BigDecimal (always works)
        TypedDataSource<BigDecimal> bigDecSource = new TypedDataSource<>(channel, reader, dataSet, BigDecimal.class);
        assertEquals(BigDecimal.valueOf(expected.longValue()), bigDecSource.readScalar());

        // Long (works if byteSize <= 8)
        TypedDataSource<Long> longSource = new TypedDataSource<>(channel, reader, dataSet, Long.class);
        if (byteSize <= 8) {
            assertEquals(expected.longValue(), longSource.readScalar());
        } else {
            assertThrows(Exception.class, longSource::readScalar);
        }

        // Integer (works if byteSize <= 4)
        TypedDataSource<Integer> intSource = new TypedDataSource<>(channel, reader, dataSet, Integer.class);
        if (byteSize <= 4) {
            assertEquals(expected.intValue(), intSource.readScalar());
        } else {
            assertThrows(Exception.class, intSource::readScalar);
        }

        // Short (works if byteSize <= 2)
        TypedDataSource<Short> shortSource = new TypedDataSource<>(channel, reader, dataSet, Short.class);
        if (byteSize <= 2) {
            assertEquals(expected.shortValue(), shortSource.readScalar());
        } else {
            assertThrows(Exception.class, shortSource::readScalar);
        }

        // Byte (works if byteSize <= 1)
        TypedDataSource<Byte> byteSource = new TypedDataSource<>(channel, reader, dataSet, Byte.class);
        if (byteSize <= 1) {
            assertEquals(expected.byteValue(), byteSource.readScalar());
        } else {
            assertThrows(Exception.class, byteSource::readScalar);
        }

        // String (always works)
        TypedDataSource<String> stringSource = new TypedDataSource<>(channel, reader, dataSet, String.class);
        assertEquals(expected.toString(), stringSource.readScalar());

        // HdfFixedPoint (always works)
        TypedDataSource<HdfFixedPoint> fixedPointSource = new TypedDataSource<>(channel, reader, dataSet, HdfFixedPoint.class);
        HdfFixedPoint fixedPoint = fixedPointSource.readScalar();
        assertEquals(expected.longValue(), fixedPoint.getInstance(Long.class).longValue());

        // HdfData (always works, same as HdfFixedPoint)
        TypedDataSource<HdfData> hdfDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
        HdfData hdfData = hdfDataSource.readScalar();
        assertEquals(expected.longValue(), hdfData.getInstance(Long.class).longValue());

        // byte[] (always works, size-specific read)
        TypedDataSource<byte[]> byteArraySource = new TypedDataSource<>(channel, reader, dataSet, byte[].class);
        byte[] byteArray = byteArraySource.readScalar();
        ByteBuffer buffer = ByteBuffer.wrap(byteArray).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        long value = switch (byteSize) {
            case 1 -> buffer.get();
            case 2 -> buffer.getShort();
            case 4 -> buffer.getInt();
            case 8 -> buffer.getLong();
            default -> throw new IllegalArgumentException("Unexpected byte size: " + byteSize);
        };
        assertEquals(expected.longValue(), value);
    }

    @Test
    void testVectorH5() throws IOException {
        Path filePath = getResourcePath("vector.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.findDataset("vector", channel, reader.getRootGroup());
            TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(channel, reader, dataSet, BigInteger.class);

            BigInteger[] vector = dataSource.readVector();
            assertArrayEquals(VECTOR_EXPECTED, vector);

            List<BigInteger> streamed = dataSource.streamVector().toList();
            assertEquals(Arrays.asList(VECTOR_EXPECTED), streamed);

            List<BigInteger> parallelStreamed = dataSource.parallelStreamVector().toList();
            assertEquals(Arrays.asList(VECTOR_EXPECTED), parallelStreamed);

            BigInteger[] flattened = dataSource.readFlattened();
            assertEquals(1000, flattened.length);
            assertArrayEquals(VECTOR_EXPECTED, flattened);

            BigInteger max = (BigInteger) FlattenedArrayUtils.reduceAlongAxis(
                    dataSource.streamFlattened(), dataSource.getShape(), 0, BigInteger::max, BigInteger.class);
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
            BigDecimal[] actualFirstRow = Arrays.stream(matrix[0])
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            assertArrayEquals(WEATHER_FIRST_ROW_EXPECTED, actualFirstRow);

            List<BigDecimal[]> streamed = dataSource.streamMatrix().toList();
            assertEquals(4, streamed.size());
            assertEquals(17, streamed.get(0).length);
            assertArrayEquals(WEATHER_FIRST_ROW_EXPECTED, Arrays.stream(streamed.get(0))
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new));

            List<BigDecimal[]> parallelStreamed = dataSource.parallelStreamMatrix().toList();
            assertEquals(4, parallelStreamed.size());
            assertEquals(17, parallelStreamed.get(0).length);
            assertArrayEquals(WEATHER_FIRST_ROW_EXPECTED, Arrays.stream(parallelStreamed.get(0))
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new));

            BigDecimal[] flattened = dataSource.readFlattened();
            assertEquals(4 * 17, flattened.length);
            assertEquals(WEATHER_FIRST_ROW_EXPECTED[0], flattened[0].setScale(2, RoundingMode.HALF_UP));

            BigDecimal[] reduced = (BigDecimal[]) FlattenedArrayUtils.reduceAlongAxis(
                    dataSource.streamFlattened(), dataSource.getShape(), 0, BigDecimal::max, BigDecimal.class);
            BigDecimal[] actualReduced = Arrays.stream(reduced)
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP))
                    .toArray(BigDecimal[]::new);
            assertArrayEquals(WEATHER_MAX_EXPECTED, actualReduced);
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
            assertArrayEquals(TICTACTOE_SHAPE_EXPECTED, shape);

            Integer[] flattened = dataSource.readFlattened();
            assertEquals(3 * 3 * 3 * 5, flattened.length);

            List<Integer> streamed = dataSource.streamFlattened().toList();
            assertEquals(3 * 3 * 3 * 5, streamed.size());

            List<Integer> parallelStreamed = dataSource.parallelStreamFlattened().toList();
            assertEquals(3 * 3 * 3 * 5, parallelStreamed.size());

            int[][] sliceStep0 = {{}, {}, {}, {0}};
            Integer[][][] step0 = (Integer[][][]) FlattenedArrayUtils.sliceStream(
                    dataSource.streamFlattened(), dataSource.getShape(), sliceStep0, Integer.class);
            assertEquals(1, step0[0][0][0]);

            List<FlattenedArrayUtils.MatchingEntry<Integer>> pieces = FlattenedArrayUtils.filterToCoordinateList(
                    dataSource.streamFlattened(), dataSource.getShape(), i -> i != 0);
            assertEquals(15, pieces.size());

            pieces.sort((a, b) -> Arrays.compare(a.coordinates, b.coordinates));
            FlattenedArrayUtils.MatchingEntry<Integer> firstPiece = pieces.get(0);
            assertArrayEquals(TICTACTOE_FIRST_PIECE_COORDS, firstPiece.coordinates);
            assertEquals(1, firstPiece.value);
            FlattenedArrayUtils.MatchingEntry<Integer> lastPiece = pieces.get(pieces.size() - 1);
            assertArrayEquals(TICTACTOE_LAST_PIECE_COORDS, lastPiece.coordinates);
            assertEquals(2, lastPiece.value);
        }
    }
}