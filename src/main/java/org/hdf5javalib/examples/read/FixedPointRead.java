package org.hdf5javalib.examples.read;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.hdf5javalib.utils.FlattenedArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hdf5javalib.utils.HdfReadUtils.getResourcePath;

/**
 * Demonstrates reading and processing fixed-point data from HDF5 files.
 * <p>
 * The {@code FixedPointRead} class is an example application that reads
 * fixed-point datasets from HDF5 files with different dimensionalities (scalar,
 * vector, matrix, and 4D). It uses {@link TypedDataSource} to process the data,
 * showcasing various operations such as reading, streaming, flattening, slicing,
 * and filtering.
 * </p>
 */
public class FixedPointRead {
    private static final Logger log = LoggerFactory.getLogger(FixedPointRead.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     * @throws IOException if an I/O error occurs
     */
    public static void main(String[] args) throws Exception {
        new FixedPointRead().run();
    }

    /**
     * Executes the main logic of reading and processing fixed-point data from HDF5 files.
     *
     * @throws IOException if an I/O error occurs
     */
    void run() throws Exception {
        Path filePath = getResourcePath("dsgroup.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader hdfFileReader = new HdfFileReader(channel).readFile();
            log.debug("File BTree: {} ", hdfFileReader.getBTree());
            for (HdfDataset dataset : hdfFileReader.getDatasets()) {
                tryScalarDataSpliterator(channel, hdfFileReader, dataset);
            }
        }

        try {
            filePath = getResourcePath("scalar.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                log.debug("File BTree: {} ", reader.getBTree());
                tryScalarDataSpliterator(channel, reader, reader.getDatasets().get(0));
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        try {
            filePath = getResourcePath("weatherdata.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                tryMatrixSpliterator(channel, reader, reader.getDataset("/weatherdata").orElseThrow());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        try {
            filePath = getResourcePath("tictactoe_4d_state.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                display4DData(channel, reader, reader.getDataset("/game").orElseThrow());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Processes a scalar dataset using a TypedDataSource.
     *
     * @param channel     the file channel for reading the HDF5 file
     * @param hdfDataFile the HDF5 file context
     * @param dataSet     the scalar dataset to process
     * @throws IOException if an I/O error occurs
     */
    void tryDataSpliterator(SeekableByteChannel channel, HdfDataFile hdfDataFile, HdfDataset dataSet) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(channel, hdfDataFile, dataSet, BigInteger.class);
        BigInteger allData = dataSource.readScalar();
        log.info("Scalar dataset name = {}", dataSet.getObjectName());
        log.info("Scalar readAll stats = {}", Stream.of(allData)
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        log.info("Scalar streaming list = {}", dataSource.streamScalar().toList());
        log.info("Scalar parallelStreaming list = {}", dataSource.parallelStreamScalar().toList());

        new TypedDataSource<>(channel, hdfDataFile, dataSet, HdfFixedPoint.class).streamScalar().forEach(item -> log.info("{}", item));
        new TypedDataSource<>(channel, hdfDataFile, dataSet, String.class).streamScalar().forEach(item -> log.info("{}", item));
        new TypedDataSource<>(channel, hdfDataFile, dataSet, BigDecimal.class).streamScalar().forEach(item -> log.info("{}", item));
    }

    /**
     * Processes a scalar dataset using a TypedDataSource.
     *
     * @param channel     the file channel for reading the HDF5 file
     * @param hdfDataFile the HDF5 file context
     * @param dataSet     the scalar dataset to process
     * @throws IOException if an I/O error occurs
     */
    void tryScalarDataSpliterator(SeekableByteChannel channel, HdfDataFile hdfDataFile, HdfDataset dataSet) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(channel, hdfDataFile, dataSet, BigInteger.class);
        BigInteger allData = dataSource.readScalar();
        log.info("Scalar dataset name = {}", dataSet.getObjectName());
        log.info("Scalar readAll stats = {}", Stream.of(allData)
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        log.info("Scalar streaming list = {}", dataSource.streamScalar().toList());
        log.info("Scalar parallelStreaming list = {}", dataSource.parallelStreamScalar().toList());

        new TypedDataSource<>(channel, hdfDataFile, dataSet, HdfFixedPoint.class).streamScalar().forEach(item -> log.info("{}", item));
        new TypedDataSource<>(channel, hdfDataFile, dataSet, String.class).streamScalar().forEach(item -> log.info("{}", item));
        new TypedDataSource<>(channel, hdfDataFile, dataSet, BigDecimal.class).streamScalar().forEach(item -> log.info("{}", item));
    }

    /**
     * Processes a vector dataset using a TypedDataSource.
     *
     * @param fileChannel the file channel for reading the HDF5 file
     * @param hdfDataFile the HDF5 file context
     * @param dataSet     the vector dataset to process
     * @throws IOException if an I/O error occurs
     */
    void tryVectorSpliterator(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfDataset dataSet) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, BigInteger.class);
        BigInteger[] allData = dataSource.readVector();
        log.info("Vector readAll stats  = {}", Arrays.stream(allData).collect(Collectors.summarizingInt(BigInteger::intValue)));
        log.info("Vector streaming stats = {}", dataSource.streamVector()
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        log.info("Vector parallel streaming stats = {}", dataSource.parallelStreamVector()
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        final BigInteger[] flattenedData = dataSource.readFlattened();
        int[] shape = dataSource.getShape();
        log.info("Vector flattenedData stats = {}", IntStream.rangeClosed(0, FlattenedArrayUtils.totalSize(shape) - 1)
                .mapToObj(i -> FlattenedArrayUtils.getElement(flattenedData, shape, i))
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        BigInteger bdReduced = (BigInteger) FlattenedArrayUtils.reduceAlongAxis(dataSource.streamFlattened(), shape, 0, BigInteger::max, BigInteger.class);
        log.info("FlattenedData Streamed Reduced = {}", bdReduced);
    }

    /**
     * Processes a matrix dataset using a TypedDataSource.
     *
     * @param fileChannel the file channel for reading the HDF5 file
     * @param hdfDataFile the HDF5 file context
     * @param dataSet     the matrix dataset to process
     * @throws IOException if an I/O error occurs
     */
    void tryMatrixSpliterator(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfDataset dataSet) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<BigDecimal> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, BigDecimal.class);
        BigDecimal[][] allData = dataSource.readMatrix();
        // Print the matrix values
        log.info("Matrix readAll() = ");
        for (BigDecimal[] allDatum : allData) {
            String row = Arrays.stream(allDatum)
                    .map(bd -> bd.setScale(2, RoundingMode.HALF_UP).toString())
                    .collect(Collectors.joining(" "));
            log.info(row);
        }

        Stream<BigDecimal[]> stream = dataSource.streamMatrix();
        // Print all values
        log.info("Matrix stream() = ");
        stream.forEach(array -> {
            String row = Arrays.stream(array)
                    .map(value -> value.setScale(2, RoundingMode.HALF_UP).toString())
                    .collect(Collectors.joining(" "));
            log.info(row);
        });

        Stream<BigDecimal[]> parallelStream = dataSource.parallelStreamMatrix();
        // Print all values in order
        log.info("Matrix parallelStream() = ");
        parallelStream.forEachOrdered(array -> {
            String row = Arrays.stream(array)
                    .map(value -> value.setScale(2, RoundingMode.HALF_UP).toString())
                    .collect(Collectors.joining(" "));
            log.info(row);
        });

        final BigDecimal[] flattenedData = dataSource.readFlattened();
        int[] shape = dataSource.getShape();
        log.info("FlattenedData = ");
        for (int r = 0; r < shape[0]; r++) {
            StringBuilder row = new StringBuilder();
            for (int c = 0; c < shape[1]; c++) {
                row.append(FlattenedArrayUtils.getElement(flattenedData, shape, r, c).setScale(2, RoundingMode.HALF_UP)).append(" ");
            }
            log.info(row.toString().trim());
        }

        log.info("FlattenedData Streamed = ");
        BigDecimal[][] bdMatrix = (BigDecimal[][]) FlattenedArrayUtils.streamToNDArray(dataSource.streamFlattened(), shape, BigDecimal.class);
        for (int r = 0; r < shape[0]; r++) {
            StringBuilder row = new StringBuilder();
            for (int c = 0; c < shape[1]; c++) {
                row.append(bdMatrix[r][c].setScale(2, RoundingMode.HALF_UP)).append(" ");
            }
            log.info(row.toString().trim());
        }

        BigDecimal[] bdReduced = (BigDecimal[]) FlattenedArrayUtils.reduceAlongAxis(dataSource.streamFlattened(), shape, 0, BigDecimal::max, BigDecimal.class);
        String reducedRow = Arrays.stream(bdReduced)
                .map(bd -> bd.setScale(2, RoundingMode.HALF_UP).toString())
                .collect(Collectors.joining(" "));
        log.info("FlattenedData Streamed Reduced = {}", reducedRow);
    }

    /**
     * Processes a 4D dataset using a TypedDataSource, demonstrating slicing and filtering.
     *
     * @param fileChannel the file channel for reading the HDF5 file
     * @param hdfDataFile the HDF5 file context
     * @param dataSet     the 4D dataset to process
     * @throws IOException if an I/O error occurs
     */
    void display4DData(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfDataset dataSet) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<Integer> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, Integer.class);
        // Print all values in order
        final Integer[] flattenedData = dataSource.readFlattened();
        int[] shape = dataSource.getShape();
        log.info("FlattenedData = ");
        for (int s = 0; s < shape[3]; s++) {
            for (int x = 0; x < shape[0]; x++) {
                for (int y = 0; y < shape[1]; y++) {
                    for (int z = 0; z < shape[2]; z++) {
                        log.info("{} {} {} {} {}", x, y, z, s, FlattenedArrayUtils.getElement(flattenedData, shape, x, y, z, s));
                    }
                }
            }
        }

        // Get cube at step 0
        int[][] sliceStep0 = {
                {},    // full x
                {},    // full y
                {},    // full z
                {0}    // step 0
        };
        Integer[][][] step0 = (Integer[][][]) FlattenedArrayUtils.sliceStream(dataSource.streamFlattened(), dataSource.getShape(), sliceStep0, Integer.class);
        log.info("Step 0:");
        for (int x = 0; x < shape[0]; x++) {
            for (int y = 0; y < shape[1]; y++) {
                for (int z = 0; z < shape[2]; z++) {
                    Integer value = step0[x][y][z];
                    log.info("({} {} {}) {}", x, y, z, value);
                }
            }
        }

        log.info("Pieces = ");
        List<FlattenedArrayUtils.MatchingEntry<Integer>> pieces = FlattenedArrayUtils.filterToCoordinateList(dataSource.streamFlattened(), shape, i -> i != 0);
        pieces.sort(Comparator.comparingInt(a -> a.coordinates[3]));
        pieces.forEach(entry -> log.info("Coords {} → Value: {}", Arrays.toString(entry.coordinates), entry.value));
    }

    /**
     * Processes a 4D dataset using a TypedDataSource, demonstrating slicing and filtering.
     *
     * @param fileChannel the file channel for reading the HDF5 file
     * @param hdfDataFile the HDF5 file context
     * @param dataSet     the 4D dataset to process
     * @throws IOException if an I/O error occurs
     */
    void displaySalesCube(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfDataset dataSet) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<Double> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, Double.class);
        int[] shape = dataSource.getShape(); // Should be [60, 100, 50]
        Double[][] sales2024Jan = (Double[][]) FlattenedArrayUtils.sliceStream(
                dataSource.streamFlattened(),
                shape,
                new int[][]{{0}, {}, {}}, // Slice Time=2024-01 (index 0)
                Double.class
        );
        log.info("Sales for January 2024:");
        for (int z = 0; z < shape[1]; z++) {
            for (int p = 0; p < shape[2]; p++) {
                log.info(String.format("Zip %d, Product %d: %.2f", z, p, sales2024Jan[z][p]));
            }
        }
    }
}