package org.hdf5javalib.examples.write;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.examples.MemorySeekableByteChannel;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfTestWriteUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public class HdfWriteTwentyTest {
    private static final Logger logger = LoggerFactory.getLogger(HdfWriteTwentyTest.class);

    private static Path getReferencePath(String fileName) {
        String resourcePath = Objects.requireNonNull(HdfWriteTwentyTest.class.getClassLoader().getResource(fileName)).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @Test
    void testWriteTwentyDatasets() throws IOException {
        logger.info("Running test: WriteTwentyDatasets");
        int headerSizeEstimate = 10232; // Conservative estimate for 20 datasets
        try (MemorySeekableByteChannel memoryChannel = new MemorySeekableByteChannel(headerSizeEstimate)) {
            HdfFile file = new HdfFile(memoryChannel);
            // Define scalar dataspace (rank 0).
            HdfFixedPoint[] hdfDimensions = {};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointWrite.computeDataSpaceMessageSize(hdfDimensions));
            for( int i=1; i<=20; i++) {
                writeInteger(i, "dataset_"+i, file, dataSpaceMessage);
            }

            file.close();
            file.getFileAllocation().printBlocksSorted();

            byte[] javaBytes = memoryChannel.toByteArray();
            Path refPath = getReferencePath("twenty_datasets.h5");
            byte[] cppBytes = Files.readAllBytes(refPath);

            // Placeholder: 20 timestamp offsets
            int[] timestampOffsets = new int[20];
            timestampOffsets[0] = 0x394;
            timestampOffsets[1] = 0x5EC;
            timestampOffsets[2] = 0x6FC;
            timestampOffsets[3] = 0x1074;
            timestampOffsets[4] = 0x1184;
            timestampOffsets[5] = 0x1294;
            timestampOffsets[6] = 0x1454;
            timestampOffsets[7] = 0x1564;
            timestampOffsets[8] = 0x1674;
            timestampOffsets[9] = 0x18CC;
            timestampOffsets[10] = 0x1B3C;
            timestampOffsets[11] = 0x1C4C;
            timestampOffsets[12] = 0x1D5C;
            timestampOffsets[13] = 0x1E6C;
            timestampOffsets[14] = 0x20C4;
            timestampOffsets[15] = 0x21D4;
            timestampOffsets[16] = 0x22E4;
            timestampOffsets[17] = 0x23F4;
            timestampOffsets[18] = 0x264C;
            timestampOffsets[19] = 0x275C;

            HdfTestWriteUtils.compareByteArraysWithTimestampExclusion(javaBytes, cppBytes, timestampOffsets);
            logger.info("Test WriteTwentyDatasets passed");
        } catch (Exception e) {
            logger.error("Test WriteTwentyDatasets failed", e);
            throw e;
        }
    }

    private static void writeInteger(int count, String datasetName, HdfFile hdfFile, DataspaceMessage dataSpaceMessage) throws IOException {
        FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField( false, false, false, true),
                (short)4, (short)0, (short)32);
        // Create dataset
        HdfDataSet dataset = hdfFile.createDataSet(datasetName, fixedPointDatatype, dataSpaceMessage);
//        HdfDisplayUtils.writeVersionAttribute(hdfFile, dataset);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(count);
        byteBuffer.flip();
        dataset.write(byteBuffer);
        dataset.close();
    }

}