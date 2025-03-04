package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.DataClassDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Hello world!
 *
 */
public class HdStringApp {
    public static void main(String[] args) {
        new HdStringApp().run();
    }
    private void run() {
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = HdfCompoundApp.class.getResource("/ascii_dataset.h5").getFile();
            try(FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                tryStringSpliterator(channel, reader);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        tryHdfApiCompound();
//        tryHdfApiInts();
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = HdfCompoundApp.class.getResource("/utf8_dataset.h5").getFile();
            try(FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                tryStringSpliterator(channel, reader);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        tryHdfApiCompound();
//        tryHdfApiInts();
    }

    private void tryStringSpliterator(FileChannel fileChannel, HdfFileReader reader) throws IOException {

        DataClassDataSource<HdfString> dataSource = new DataClassDataSource<>(reader.getDataObjectHeaderPrefix(), 0, fileChannel, reader.getDataAddress(), HdfString.class);
        System.out.println("String stream = " + dataSource.stream().toList());

        HdfString[] allData = dataSource.readAll();
        System.out.println("String stream = " + Arrays.stream(allData).toList());

    }

//    public void tryHdfApiInts() {
//        final String FILE_NAME = "randomints.h5";
//        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
//        final String DATASET_NAME = "temperature";
//        final int NUM_RECORDS = 100;
//
//        try {
//            // Create a new HDF5 file
//            HdfFile file = new HdfFile(FILE_NAME, FILE_OPTIONS);
//
//            // Create data space
//            HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(NUM_RECORDS)};
//            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, false);
////            hsize_t dim[1] = { NUM_RECORDS };
////            DataSpace space(1, dim);
//
//            FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
//                    FixedPointDatatype.createClassAndVersion(),
//                    FixedPointDatatype.createClassBitField( false, false, false, true),
//                    (short)8, (short)0, (short)64);
//
//            // Create dataset
////            DataSet dataset = file.createDataSet(DATASET_NAME, compoundType, space);
//            HdfDataSet dataset = file.createDataSet(DATASET_NAME, fixedPointDatatype, dataSpaceMessage);
//
//            writeVersionAttribute(dataset);
//
//            AtomicInteger countHolder = new AtomicInteger(0);
//            FixedPointTypedDataSource<TemperatureData> temperatureDataHdfDataSource = new FixedPointTypedDataSource<>(dataset.getDataObjectHeaderPrefix(), "temperature", 0, TemperatureData.class);
//            ByteBuffer temperatureBuffer = ByteBuffer.allocate(fixedPointDatatype.getSize());
//            // Write to dataset
//            dataset.write(() -> {
//                int count = countHolder.getAndIncrement();
//                if (count >= NUM_RECORDS) return  ByteBuffer.allocate(0);
//                TemperatureData instance = TemperatureData.builder()
//                        .temperature(BigInteger.valueOf((long) (Math.random() *40.0 + 10.0)))
//                        .build();
//                temperatureBuffer.clear();
//                temperatureDataHdfDataSource.writeToBuffer(instance, temperatureBuffer);
//                temperatureBuffer.flip();
//                return temperatureBuffer;
//            });
//            dataset.close();
//            file.close();
//
//            // auto close
//
//            System.out.println("HDF5 file created and written successfully!");
//        } catch (Exception e) {
//            System.out.println(e.getMessage());
//        }
//    }

    private void writeVersionAttribute(HdfDataSet dataset) {
        String ATTRIBUTE_NAME = "GIT root revision";
        String ATTRIBUTE_VALUE = "Revision: , URL: ";
        BitSet classBitField = StringDatatype.getStringTypeBitSet(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII);
        // value
        StringDatatype attributeType = new StringDatatype(StringDatatype.createClassAndVersion(), classBitField, (short)ATTRIBUTE_VALUE.length());
        // data type, String, DATASET_NAME.length
        DatatypeMessage dt = new DatatypeMessage(attributeType);
        // scalar, 1 string
        DataspaceMessage ds = new DataspaceMessage(1, 0, 0, null, null, false);
        HdfString hdfString = new HdfString(ATTRIBUTE_VALUE.getBytes(), classBitField);
        dataset.createAttribute(ATTRIBUTE_NAME, dt, ds, hdfString);
    }

}
