package org.hdf5javalib.examples;

import org.hdf5javalib.dataclass.HdfFixedPoint;
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

public class HdfWriteTwentyComparisonTest {
    private static final Logger logger = LoggerFactory.getLogger(HdfWriteTwentyComparisonTest.class);

    private static Path getReferencePath(String fileName) {
        String resourcePath = Objects.requireNonNull(HdfWriteTwentyComparisonTest.class.getClassLoader().getResource(fileName)).getPath();
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
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointApp.computeDataSpaceMessageSize(hdfDimensions));
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
            timestampOffsets[1] = 0x394;
            timestampOffsets[2] = 0x394;
            timestampOffsets[3] = 0x394;
            timestampOffsets[4] = 0x394;
            timestampOffsets[5] = 0x394;
            timestampOffsets[6] = 0x394;
            timestampOffsets[7] = 0x394;
            timestampOffsets[8] = 0x394;
            timestampOffsets[9] = 0x394;
            timestampOffsets[10] = 0x394;
            timestampOffsets[11] = 0x394;
            timestampOffsets[12] = 0x394;
            timestampOffsets[13] = 0x394;
            timestampOffsets[14] = 0x394;
            timestampOffsets[15] = 0x394;
            timestampOffsets[16] = 0x394;
            timestampOffsets[17] = 0x394;
            timestampOffsets[18] = 0x394;
            timestampOffsets[19] = 0x394;

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

        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(count);
        byteBuffer.flip();
        dataset.write(byteBuffer);
        dataset.close();
    }

}