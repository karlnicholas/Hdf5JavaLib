package org.hdf5javalib.examples;

import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfTestWriteUtils;
import org.hdf5javalib.utils.HdfWriteUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class HdfWriteComparisonTest {
    private static final Logger logger = LoggerFactory.getLogger(HdfWriteComparisonTest.class);

    private static Path getReferencePath(String fileName) {
        String resourcePath = Objects.requireNonNull(HdfWriteComparisonTest.class.getClassLoader().getResource(fileName)).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("provideTestConfigurations")
    void testWriteMatchesCpp(String testName, String refFile, String datasetName, int[] dimensions, FixedPointDatatype datatype, int timestampOffset, BiConsumer<HdfDataSet, HdfFile> writer) throws IOException {
        logger.info("Running test: {}", testName);
        int dataSize = dimensions[0] * (dimensions.length > 1 ? dimensions[1] : 1) * datatype.getSize();
        int headerSizeEstimate = 2048;
        try (MemorySeekableByteChannel memoryChannel = new MemorySeekableByteChannel(headerSizeEstimate + dataSize)) {
            HdfFile file = new HdfFile(memoryChannel);
            HdfFixedPoint[] hdfDimensions = new HdfFixedPoint[dimensions.length];
            for (int i = 0; i < dimensions.length; i++) {
                hdfDimensions[i] = HdfFixedPoint.of(dimensions[i]);
            }
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(
                    1, (byte) dimensions.length, DataspaceMessage.buildFlagSet(true, false),
                    hdfDimensions, hdfDimensions, false, (byte) 0, computeDataSpaceMessageSize(hdfDimensions));
            HdfDataSet dataset = file.createDataSet(datasetName, datatype, dataSpaceMessage);
            HdfDisplayUtils.writeVersionAttribute(file, dataset);
            writer.accept(dataset, file);
            dataset.close();
            file.close();

            byte[] javaBytes = memoryChannel.toByteArray();
            Path refPath = getReferencePath(refFile);
            byte[] cppBytes = Files.readAllBytes(refPath);

            HdfTestWriteUtils.compareByteArraysWithTimestampExclusion(javaBytes, cppBytes, timestampOffset);
            logger.info("Test {} passed", testName);
        } catch (Exception e) {
            logger.error("Test {} failed", testName, e);
            throw e;
        }
    }

    @SneakyThrows
    private static void writeVectorAll(HdfDataSet dataset, HdfDataFile hdfDataFile) {
        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfDataFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfDataFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfDataFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        int numRecords = 1000;
        ByteBuffer byteBuffer = ByteBuffer.allocate(numRecords * 8).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < numRecords; i++) {
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, HdfFixedPoint.of(i + 1));
        }
        byteBuffer.flip();
        dataset.write(byteBuffer);
    }

    @SneakyThrows
    private static void writeVectorEach(HdfDataSet dataset, HdfDataFile hdfDataFile) {
        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfDataFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfDataFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfDataFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        int numRecords = 1000;
        AtomicInteger countHolder = new AtomicInteger(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        dataset.write(() -> {
            int count = countHolder.getAndIncrement();
            if (count >= numRecords) return ByteBuffer.allocate(0);
            byteBuffer.clear();
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, HdfFixedPoint.of(count + 1));
            byteBuffer.flip();
            return byteBuffer;
        });
    }

    @SneakyThrows
    private static void writeMatrixAll(HdfDataSet dataset, HdfDataFile hdfDataFile) {
        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfDataFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfDataFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfDataFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        int numRecords = 4;
        int numDatapoints = 17;
        List<List<BigDecimal>> values = loadCsvData("/weatherdata.csv");
        ByteBuffer byteBuffer = ByteBuffer.allocate(numRecords * numDatapoints * 4).order(ByteOrder.LITTLE_ENDIAN);
        BigDecimal twoShifted = new BigDecimal(BigInteger.ONE.shiftLeft(7));
        BigDecimal point5 = new BigDecimal("0.5");
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                (short) 4, (short) 7, (short) 25);
        for (int r = 0; r < numRecords; r++) {
            for (int c = 0; c < numDatapoints; c++) {
                BigDecimal rawValue = values.get(r).get(c).multiply(twoShifted).add(point5);
                BigInteger rawValueShifted = rawValue.toBigInteger();
                new HdfFixedPoint(rawValueShifted, datatype).writeValueToByteBuffer(byteBuffer);
            }
        }
        byteBuffer.flip();
        dataset.write(byteBuffer);
    }

    @SneakyThrows
    private static void writeMatrixEach(HdfDataSet dataset, HdfDataFile hdfDataFile) {
        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfDataFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfDataFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfDataFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        int numRecords = 4;
        int numDatapoints = 17;
        List<List<BigDecimal>> values = loadCsvData("/weatherdata.csv");
        AtomicInteger countHolder = new AtomicInteger(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(numDatapoints * 4).order(ByteOrder.LITTLE_ENDIAN);
        BigDecimal twoShifted = new BigDecimal(BigInteger.ONE.shiftLeft(7));
        BigDecimal point5 = new BigDecimal("0.5");
        FixedPointDatatype datatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                (short) 4, (short) 7, (short) 25);
        dataset.write(() -> {
            int count = countHolder.getAndIncrement();
            if (count >= numRecords) return ByteBuffer.allocate(0);
            byteBuffer.clear();
            for (int c = 0; c < numDatapoints; c++) {
                BigDecimal rawValue = values.get(count).get(c).multiply(twoShifted).add(point5);
                BigInteger rawValueShifted = rawValue.toBigInteger();
                new HdfFixedPoint(rawValueShifted, datatype).writeValueToByteBuffer(byteBuffer);
            }
            byteBuffer.flip();
            return byteBuffer;
        });
    }

    private static List<List<BigDecimal>> loadCsvData(String filePath) throws IOException {
        List<String> labels;
        List<List<BigDecimal>> data = new ArrayList<>();
        try (Reader reader = new InputStreamReader(Objects.requireNonNull(HdfWriteComparisonTest.class.getResourceAsStream(filePath)), StandardCharsets.UTF_8)) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build();
            CSVParser parser = new CSVParser(reader, csvFormat);
            labels = new ArrayList<>(parser.getHeaderNames());
            for (CSVRecord record : parser) {
                List<BigDecimal> row = new ArrayList<>();
                for (String label : labels) {
                    String value = record.get(label);
                    row.add(value != null && !value.isEmpty() ? new BigDecimal(value).setScale(2, RoundingMode.HALF_UP) : null);
                }
                data.add(row);
            }
        }
        return data;
    }

    public static short computeDataSpaceMessageSize(HdfFixedPoint[] hdfDimensions) {
        short dataSpaceMessageSize = 8;
        if (hdfDimensions != null) {
            for (HdfFixedPoint dimension : hdfDimensions) {
                dataSpaceMessageSize += (short) dimension.getDatatype().getSize();
            }
            for (HdfFixedPoint maxDimension : hdfDimensions) {
                dataSpaceMessageSize += (short) maxDimension.getDatatype().getSize();
            }
        }
        return dataSpaceMessageSize;
    }

    private static Stream<Arguments> provideTestConfigurations() {
        FixedPointDatatype vectorDatatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, true),
                (short) 8, (short) 0, (short) 64);
        FixedPointDatatype matrixDatatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                (short) 4, (short) 7, (short) 25);
        return Stream.of(
                Arguments.of("BulkWrite_Vector_1000", "vector.h5", "vector", new int[]{1000}, vectorDatatype, 932, (BiConsumer<HdfDataSet, HdfFile>) HdfWriteComparisonTest::writeVectorAll),
                Arguments.of("IncrementalWrite_Vector_1000", "vector.h5", "vector", new int[]{1000}, vectorDatatype, 932, (BiConsumer<HdfDataSet, HdfFile>) HdfWriteComparisonTest::writeVectorEach),
                Arguments.of("BulkWrite_Matrix_4x17", "weatherdata.h5", "weatherdata", new int[]{4, 17}, matrixDatatype, 0x322, (BiConsumer<HdfDataSet, HdfFile>) HdfWriteComparisonTest::writeMatrixAll),
                Arguments.of("IncrementalWrite_Matrix_4x17", "weatherdata.h5", "weatherdata", new int[]{4, 17}, matrixDatatype, 0x322, (BiConsumer<HdfDataSet, HdfFile>) HdfWriteComparisonTest::writeMatrixEach)
        );
    }
}