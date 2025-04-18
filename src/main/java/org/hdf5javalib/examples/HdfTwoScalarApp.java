package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Hello world!
 *
 */
public class HdfTwoScalarApp {
    public static void main(String[] args) {
        new HdfTwoScalarApp().run();
    }

    private void run() {
//        try {
//            Path filePath = getResourcePath("two_scalar_datasets.h5");
//            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
//                HdfFileReader reader = new HdfFileReader(channel).readFile();
////                for( HdfDataSet dataSet: reader.getDatasets(channel, reader.getRootGroup())) {
//                    HdfDataSet ds = reader.findDataset("FixedPointValue", channel, reader.getRootGroup());
//                    HdfDisplayUtils.displayScalarData(channel, ds, Long.class, reader);
////                    try ( HdfDataSet ds = dataSet) {
////                        HdfDisplayUtils.displayScalarData(channel, ds, Long.class, reader);
////                    }
////                }
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        tryHdfApiScalar("two_scalar_datasets.h5");
    }

    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }


    private void tryHdfApiScalar(String FILE_NAME) {
        final String DATASET_NAME = "dataset_";

        try (HdfFile hdfFile = new HdfFile(Files.newByteChannel(Path.of(FILE_NAME), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
            // Create a new file using default properties


            // Define scalar dataspace (rank 0).
            HdfFixedPoint[] hdfDimensions = {};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointApp.computeDataSpaceMessageSize(hdfDimensions));


            FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField( false, false, false, true),
                    (short)4, (short)0, (short)32);


            //            for ( int i = 1; i <= 2; i++ ) {
                // Create dataset
                HdfDataSet dataset = hdfFile.createDataSet("FixedPointValue", fixedPointDatatype, dataSpaceMessage);
                HdfDisplayUtils.writeVersionAttribute(hdfFile, dataset);

                HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
                hdfFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
                boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
                if (requiresGlobalHeap) {
                    if (!hdfFile.getFileAllocation().hasGlobalHeapAllocation()) {
                        hdfFile.getFileAllocation().allocateFirstGlobalHeapBlock();
                    }
                }

                ByteBuffer byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                byteBuffer.putInt(42);
//                , fixedPointDatatype
//                HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, fixedPointDatatype.getInstance(HdfFixedPoint.class, Integer.valueOf(42).) );
                byteBuffer.flip();
                // Write to dataset
                dataset.write(byteBuffer);

//                dataset.close();
//            }
//
//            file.close();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // auto close
        System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
    }

}