package org.hdf5javalib.examples;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Hello world!
 *
 */
public class HdfTwentyScalarApp {
    public static void main(String[] args) {
        new HdfTwentyScalarApp().run();
    }

    private void run() {
//        try {
//            Path filePath = getResourcePath("twenty_datasets.h5");
//            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
//                HdfFileReader reader = new HdfFileReader(channel).readFile();
//                for( HdfDataSet dataSet: reader.getRootGroup().getDataSets()) {
//                    try ( HdfDataSet ds = dataSet) {
//                        HdfDisplayUtils.displayScalarData(channel, ds, Long.class, reader);
//                    }
//                }
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        tryHdfApiScalar("twenty_datasets.h5");
    }

    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }


    private void tryHdfApiScalar(String FILE_NAME) {
        // Create a new file using default properties
        try (HdfFile hdfFile = new HdfFile(Files.newByteChannel(Path.of(FILE_NAME), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
            // Define scalar dataspace (rank 0).
            HdfFixedPoint[] hdfDimensions = {};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointApp.computeDataSpaceMessageSize(hdfDimensions));
            for( int i=1; i<=20; i++) {
                writeInteger(i, "dataset_"+i, hdfFile, dataSpaceMessage);
                hdfFile.getFileAllocation().printBlocks();
            }

            hdfFile.close();
            hdfFile.getFileAllocation().printBlocksSorted();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // auto close
        System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
    }


//    private void tryHdfApiScalar(String FILE_NAME) {
//        // Create a new file using default properties
//        try (HdfFile hdfFile = new HdfFile(Files.newByteChannel(Path.of(FILE_NAME), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
//            // Define scalar dataspace (rank 0).
//            HdfFixedPoint[] hdfDimensions = {};
//            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointApp.computeDataSpaceMessageSize(hdfDimensions));
//
//            hdfFile.getFileAllocation().printBlocks();
//            writeByte(hdfFile, dataSpaceMessage);
//            hdfFile.getFileAllocation().printBlocks();
//            writeShort(hdfFile, dataSpaceMessage);
//            hdfFile.getFileAllocation().printBlocks();
//            writeInteger(hdfFile, dataSpaceMessage);
//            hdfFile.getFileAllocation().printBlocks();
//            writeLong(hdfFile, dataSpaceMessage);
//            hdfFile.getFileAllocation().printBlocks();
//
//            hdfFile.close();
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        // auto close
//        System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
//    }

    private static void writeByte(HdfFile hdfFile, DataspaceMessage dataSpaceMessage) throws IOException {
        FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField( false, false, false, true),
                (short)1, (short)0, (short)8);
        // Create dataset
        HdfDataSet dataset = hdfFile.createDataSet("byte", fixedPointDatatype, dataSpaceMessage);
        HdfDisplayUtils.writeVersionAttribute(hdfFile, dataset);

        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(1).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put((byte) 42);
        byteBuffer.flip();
        dataset.write(byteBuffer);
        dataset.close();
    }

    private static void writeShort(HdfFile hdfFile, DataspaceMessage dataSpaceMessage) throws IOException {
        FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField( false, false, false, true),
                (short)2, (short)0, (short)16);
        // Create dataset
        HdfDataSet dataset = hdfFile.createDataSet("short", fixedPointDatatype, dataSpaceMessage);
        HdfDisplayUtils.writeVersionAttribute(hdfFile, dataset);

        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putShort((short) 42);
        byteBuffer.flip();
        dataset.write(byteBuffer);
        dataset.close();
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

    private static void writeLong(HdfFile hdfFile, DataspaceMessage dataSpaceMessage) throws IOException {
        FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField( false, false, false, true),
                (short)8, (short)0, (short)64);
        // Create dataset
        HdfDataSet dataset = hdfFile.createDataSet("long", fixedPointDatatype, dataSpaceMessage);
        HdfDisplayUtils.writeVersionAttribute(hdfFile, dataset);

        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putLong(42);
        byteBuffer.flip();
        dataset.write(byteBuffer);
        dataset.close();
    }

}