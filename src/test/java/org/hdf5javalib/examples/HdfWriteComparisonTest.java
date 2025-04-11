package org.hdf5javalib.examples;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class HdfWriteComparisonTest {
    private static Path getReferencePath(String fileName) {
        String resourcePath = Objects.requireNonNull(HdfWriteComparisonTest.class.getClassLoader().getResource(fileName)).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("provideTestConfigurations")
    void testWriteMatchesCpp(String testName, String refFile, int numRecords, String datasetName, int timestampOffset, Consumer<WriterParams> writer) throws IOException {
        writeAndTest(refFile, numRecords, datasetName, timestampOffset, writer);
    }

    private void writeAndTest(String refFile, int numRecords, String datasetName, int timestampOffset, Consumer<WriterParams> writer) throws IOException {
        int dataSize = numRecords * 8; // 8 bytes per int64_t
        int headerSizeEstimate = 2048; // Rough estimate for header + metadata
        try (MemorySeekableByteChannel memoryChannel = new MemorySeekableByteChannel(headerSizeEstimate + dataSize)) {
            try {
                HdfFile file = new HdfFile(memoryChannel);
                HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(numRecords)};
                DataspaceMessage dataSpaceMessage = new DataspaceMessage(
                        1, 1, DataspaceMessage.buildFlagSet(true, false),
                        hdfDimensions, hdfDimensions, false, (byte) 0, computeDataSpaceMessageSize(hdfDimensions));
                FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                        FixedPointDatatype.createClassAndVersion(),
                        FixedPointDatatype.createClassBitField(false, false, false, true),
                        (short) 8, (short) 0, (short) 64);
                HdfDataSet dataset = file.createDataSet(datasetName, fixedPointDatatype, dataSpaceMessage);
                HdfDisplayUtils.writeVersionAttribute(dataset);
                writer.accept(new WriterParams(numRecords, fixedPointDatatype, dataset));
                dataset.close();
                file.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            byte[] javaBytes = memoryChannel.toByteArray();
            Path refPath = getReferencePath(refFile);
            byte[] cppBytes = Files.readAllBytes(refPath);

            HdfTestWriteUtils.compareByteArraysWithTimestampExclusion(javaBytes, cppBytes, timestampOffset);
        }
    }

    @SneakyThrows
    private static void writeAll(WriterParams writerParams) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.fixedPointDatatype.getSize() * writerParams.NUM_RECORDS)
                .order(writerParams.fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < writerParams.NUM_RECORDS; i++) {
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, HdfFixedPoint.of(i + 1));
        }
        byteBuffer.flip();
        writerParams.dataset.write(byteBuffer);
    }

    @SneakyThrows
    private static void writeEach(WriterParams writerParams) {
        AtomicInteger countHolder = new AtomicInteger(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.fixedPointDatatype.getSize())
                .order(writerParams.fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        writerParams.dataset.write(() -> {
            int count = countHolder.getAndIncrement();
            if (count >= writerParams.NUM_RECORDS) return ByteBuffer.allocate(0);
            byteBuffer.clear();
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, HdfFixedPoint.of(count + 1));
            byteBuffer.flip();
            return byteBuffer;
        });
    }

    @AllArgsConstructor
    static class WriterParams {
        int NUM_RECORDS;
        FixedPointDatatype fixedPointDatatype;
        HdfDataSet dataset;
    }

    public static short computeDataSpaceMessageSize(HdfFixedPoint[] hdfDimensions) {
        short dataSpaceMessageSize = 8;
        if (hdfDimensions != null) {
            for (HdfFixedPoint dimension : hdfDimensions) {
                dataSpaceMessageSize += dimension.getDatatype().getSize();
            }
            for (HdfFixedPoint maxDimension : hdfDimensions) {
                dataSpaceMessageSize += maxDimension.getDatatype().getSize();
            }
        }
        return dataSpaceMessageSize;
    }

    private static Stream<Arguments> provideTestConfigurations() {
        return Stream.of(
                Arguments.of("BulkWrite_Vector_1000", "vector.h5", 1000, "vector", 932, (Consumer<WriterParams>) HdfWriteComparisonTest::writeAll),
                Arguments.of("IncrementalWrite_Vector_1000", "vector.h5", 1000, "vector", 932, (Consumer<WriterParams>) HdfWriteComparisonTest::writeEach)
                // Add more configurations here, e.g.:
                // Arguments.of("BulkWrite_Small_100", "small_vector.h5", 100, "data", 512, HdfWriteComparisonTest::writeAll),
                // Arguments.of("IncrementalWrite_Large_5000", "large_vector.h5", 5000, "bigdata", 1024, HdfWriteComparisonTest::writeEach)
        );
    }
}