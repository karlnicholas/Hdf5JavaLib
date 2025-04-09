package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfTestUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = Objects.requireNonNull(this.getClass().getResource("/two_scalar_datasets.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                for( HdfDataSet dataSet: reader.getDatasets(channel, reader.getRootGroup())) {
                    try ( HdfDataSet ds = dataSet) {
                        HdfTestUtils.displayScalarData(channel, ds, Integer.class);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tryHdfApiScalar("two_scalar_datasets.h5");
    }

    private void tryHdfApiScalar(String FILE_NAME) {
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "dataset_";

        try (HdfFile file = new HdfFile(FILE_NAME, FILE_OPTIONS) ) {
            // Create a new file using default properties


            // Define scalar dataspace (rank 0).
            HdfFixedPoint[] hdfDimensions = {};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, (short) 8);

            FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField( false, false, false, true),
                    (short)4, (short)0, (short)32);

//            for ( int i = 1; i <= 2; i++ ) {
                // Create dataset
                HdfDataSet dataset = file.createDataSet(DATASET_NAME+1, fixedPointDatatype, dataSpaceMessage);

                ByteBuffer byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                HdfWriteUtils.writeBigIntegerAsHdfFixedPoint(BigInteger.valueOf((long) 42), fixedPointDatatype, byteBuffer);
                byteBuffer.flip();
                // Write to dataset
                dataset.write(byteBuffer);

//                dataset.close();
//            }
//
//            file.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // auto close
        System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
    }

}
