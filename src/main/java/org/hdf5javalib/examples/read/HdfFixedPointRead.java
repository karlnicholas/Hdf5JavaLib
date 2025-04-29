package org.hdf5javalib.examples.read;


import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.FlattenedArrayUtils;

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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
public class HdfFixedPointRead {
    public static void main(String[] args) throws IOException {
        new HdfFixedPointRead().run();
    }
    Path getResourcePath(String fileName) {
        String resourcePath = Objects.requireNonNull(getClass().getClassLoader().getResource(fileName)).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }


    void run() throws IOException {
        Path filePath = getResourcePath("scalar.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            for( HdfDataSet dataset: reader.getRootGroup().getDataSets()) {
                try (HdfDataSet ds = dataset) {
                    tryScalarDataSpliterator(channel, reader, ds);
                }
            }
        }
        try {
            filePath = getResourcePath("vector.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                tryVectorSpliterator(channel, reader, reader.getRootGroup().findDataset("vector"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            filePath = getResourcePath("weatherdata.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                tryMatrixSpliterator(channel, reader, reader.getRootGroup().findDataset("weatherdata"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            filePath = getResourcePath("tictactoe_4d_state.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                display4DData(channel, reader, reader.getRootGroup().findDataset("game"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void tryScalarDataSpliterator(SeekableByteChannel channel, HdfDataFile hdfDataFile, HdfDataSet dataSet) throws IOException {
        TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(channel, hdfDataFile, dataSet, BigInteger.class);
        BigInteger allData = dataSource.readScalar();
        System.out.println("Scalar readAll stats = " + Stream.of(allData)
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        System.out.println("Scalar streaming list = " + dataSource.streamScalar().toList());
        System.out.println("Scalar parallelStreaming list = " + dataSource.parallelStreamScalar().toList());

        new TypedDataSource<>(channel, hdfDataFile, dataSet, HdfFixedPoint.class).streamScalar().forEach(System.out::println);
        new TypedDataSource<>(channel, hdfDataFile, dataSet, String.class).streamScalar().forEach(System.out::println);
        new TypedDataSource<>(channel, hdfDataFile, dataSet, BigDecimal.class).streamScalar().forEach(System.out::println);
    }

    void tryVectorSpliterator(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfDataSet dataSet) throws IOException {
        TypedDataSource<BigInteger> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, BigInteger.class);
        BigInteger[] allData = dataSource.readVector();
        System.out.println("Vector readAll stats  = " + Arrays.stream(allData).collect(Collectors.summarizingInt(BigInteger::intValue)));
        System.out.println("Vector streaming stats = " + dataSource.streamVector()
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        System.out.println("Vector parallel streaming stats = " + dataSource.parallelStreamVector()
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        final BigInteger[] flattenedData = dataSource.readFlattened();
        int[] shape = dataSource.getShape();
        System.out.println("Vector flattenedData stats = " + IntStream.rangeClosed(0, FlattenedArrayUtils.totalSize(shape)-1).mapToObj(i-> FlattenedArrayUtils.getElement(flattenedData, shape, i)).collect(Collectors.summarizingInt(BigInteger::intValue)));
        System.out.print("FlattenedData Streamed Reduced = ");
        BigInteger bdReduced = (BigInteger) FlattenedArrayUtils.reduceAlongAxis(dataSource.streamFlattened(), shape, 0, BigInteger::max, BigInteger.class);
        System.out.println(bdReduced + " ");
    }

    void tryMatrixSpliterator(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfDataSet dataSet) throws IOException {
        TypedDataSource<BigDecimal> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, BigDecimal.class);
        BigDecimal[][] allData = dataSource.readMatrix();
        // Print the matrix values
        System.out.println("Matrix readAll() = ");
        for (BigDecimal[] allDatum : allData) {
            for (BigDecimal bigDecimal : allDatum) {
                System.out.print(bigDecimal.setScale(2, RoundingMode.HALF_UP) + " ");
            }
            System.out.println(); // New line after each row
        }

        Stream<BigDecimal[]> stream = dataSource.streamMatrix();
        // Print all values
        System.out.println("Matrix stream() = ");
        stream.forEach(array -> {
            for (BigDecimal value : array) {
                System.out.print(value.setScale(2, RoundingMode.HALF_UP) + " ");
            }
            System.out.println(); // Newline after each array
        });
        Stream<BigDecimal[]> parallelStream = dataSource.parallelStreamMatrix();

        // Print all values in order
        System.out.println("Matrix parallelStream() = ");
        parallelStream.forEachOrdered(array -> {
            for (BigDecimal value : array) {
                System.out.print(value.setScale(2, RoundingMode.HALF_UP) + " ");
            }
            System.out.println();
        });// Print all values in order
        final BigDecimal[] flattenedData = dataSource.readFlattened();
        int[] shape = dataSource.getShape();
        System.out.println("FlattenedData = ");
        for(int r = 0; r < shape[0]; r++){
            for(int c = 0; c < shape[1]; c++){
                System.out.print(FlattenedArrayUtils.getElement(flattenedData, shape, r, c).setScale(2, RoundingMode.HALF_UP) + " ");
            }
            System.out.println();
        }

        System.out.println("FlattenedData Streamed = ");
        BigDecimal[][] bdMatrix = (BigDecimal[][]) FlattenedArrayUtils.streamToNDArray(dataSource.streamFlattened(), shape, BigDecimal.class);
        for(int r = 0; r < shape[0]; r++){
            for(int c = 0; c < shape[1]; c++){
                System.out.print(bdMatrix[r][c].setScale(2, RoundingMode.HALF_UP) + " ");
            }
            System.out.println();
        }

        System.out.println("FlattenedData Streamed Reduced = ");
        BigDecimal[] bdReduced = (BigDecimal[]) FlattenedArrayUtils.reduceAlongAxis(dataSource.streamFlattened(), shape, 0, BigDecimal::max, BigDecimal.class);
        for(int c = 0; c < shape[1]; c++){
            System.out.print(bdReduced[c].setScale(2, RoundingMode.HALF_UP) + " ");
        }
    }

    void display4DData(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfDataSet dataSet) throws IOException {
        TypedDataSource<Integer> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, Integer.class);
        // Print all values in order
        final Integer[] flattenedData = dataSource.readFlattened();
        int[] shape = dataSource.getShape();
        System.out.println("FlattenedData = ");
        for (int s = 0; s < shape[3]; s++) {
            for(int x = 0; x < shape[0]; x++){
                for(int y = 0; y < shape[1]; y++){
                    for(int z = 0; z < shape[2]; z++) {
                        System.out.print(x + " " + y + " " + z + ":" + s + ":" + FlattenedArrayUtils.getElement(flattenedData, shape, x, y, z, s) + " ");
                        System.out.println();
                    }
                }
            }
        }

        // get cube at step 0
        int[][] sliceStep0 = {
                {},    // full x
                {},     // full y
                {},     // full z
                {0}  // step 0
        };
        Integer[][][] step0 = (Integer[][][])FlattenedArrayUtils.sliceStream(dataSource.streamFlattened(), dataSource.getShape(), sliceStep0, Integer.class);
        System.out.println("Step 0:");
        for (int x = 0; x < shape[0]; x++) {
            for (int y = 0; y < shape[1]; y++) {
                for (int z = 0; z < shape[2]; z++) {
                    Integer value = step0[x][y][z];
                    System.out.printf("(%d %d %d) %s%n", x, y, z, value);
                }
            }
        }

        System.out.println("Pieces = ");
        List<FlattenedArrayUtils.MatchingEntry<Integer>> pieces = FlattenedArrayUtils.filterToCoordinateList(dataSource.streamFlattened(), shape, i -> i != 0);
        pieces.sort(Comparator.comparingInt(a -> a.coordinates[3]));
        pieces.forEach(entry -> System.out.printf("Coords %s → Value: %s%n", Arrays.toString(entry.coordinates), entry.value));
    }

}
