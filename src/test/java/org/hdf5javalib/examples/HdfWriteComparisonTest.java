package org.hdf5javalib.examples;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfDebugUtils;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfWriteUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class HdfWriteComparisonTest {
    private static final String DATASET_NAME = "vector";
    private static final int NUM_RECORDS = 1000;
    private static final int TIMESTAMP_OFFSET = 932;

    private static Path getReferencePath() {
        String resourcePath = Objects.requireNonNull(HdfWriteComparisonTest.class.getClassLoader().getResource("vector.h5")).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @Test
    void testBulkWriteMatchesCpp() throws IOException {
        writeAndTest(NUM_RECORDS, DATASET_NAME, this::writeAll);
    }

    @Test
    void testIncrementalWriteMatchesCpp() throws IOException {
        writeAndTest(NUM_RECORDS, DATASET_NAME, this::writeEach);
    }

    private void writeAndTest(int numRecords, String datasetName, Consumer<WriterParams> writer) throws IOException {
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
            Path refPath = getReferencePath();
            byte[] cppBytes = Files.readAllBytes(refPath);

            compareByteArraysWithTimestampExclusion(javaBytes, cppBytes, TIMESTAMP_OFFSET);
        }
    }

    @SneakyThrows
    private void writeAll(WriterParams writerParams) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.fixedPointDatatype.getSize() * writerParams.NUM_RECORDS)
                .order(writerParams.fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < writerParams.NUM_RECORDS; i++) {
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, HdfFixedPoint.of(i + 1));
        }
        byteBuffer.flip();
        writerParams.dataset.write(byteBuffer);
    }

    @SneakyThrows
    private void writeEach(WriterParams writerParams) {
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

    private void compareByteArraysWithTimestampExclusion(byte[] javaBytes, byte[] cppBytes, int timestampOffset) throws IOException {
        if (javaBytes.length != cppBytes.length) {
            throw new AssertionError("Array lengths differ: Java=" + javaBytes.length + ", C++=" + cppBytes.length);
        }

        int diffOffset = Arrays.mismatch(javaBytes, cppBytes);
        if (diffOffset == -1 || (diffOffset >= timestampOffset && diffOffset < timestampOffset + 4)) {
            return; // No mismatch or only in timestamp
        }

        maskTimestamp(javaBytes, timestampOffset);
        maskTimestamp(cppBytes, timestampOffset);

        diffOffset = Arrays.mismatch(javaBytes, cppBytes);
        if (diffOffset != -1) {
            int windowSize = 64; // ±32 bytes
            int halfWindow = windowSize / 2; // 32
            int start = (diffOffset - halfWindow) & ~0xF; // Align to 16-byte boundary
            int end = start + windowSize;

            if (start < 0 || end > javaBytes.length) {
                throw new IllegalStateException("Dump range out of bounds: " + start + " to " + end);
            }

            byte[] javaWindow = Arrays.copyOfRange(javaBytes, start, end);
            byte[] cppWindow = Arrays.copyOfRange(cppBytes, start, end);

            System.out.println("Difference found at offset: 0x" + Integer.toHexString(diffOffset).toUpperCase());
            System.out.println("Java bytes (masked):");
            HdfDebugUtils.dumpByteBuffer(ByteBuffer.wrap(javaWindow));
            System.out.println("C++ bytes (masked):");
            HdfDebugUtils.dumpByteBuffer(ByteBuffer.wrap(cppWindow));

            throw new AssertionError("Byte arrays differ at offset 0x" + Integer.toHexString(diffOffset).toUpperCase() + " (excluding timestamp)");
        }
    }

    private void maskTimestamp(byte[] bytes, int offset) {
        for (int i = 0; i < 4; i++) { // 4-byte timestamp
            bytes[offset + i] = 0;    // Modify in place
        }
    }
}