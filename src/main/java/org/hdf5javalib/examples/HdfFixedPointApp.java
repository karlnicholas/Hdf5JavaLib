package org.hdf5javalib.examples;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.DataClassDataSource;
import org.hdf5javalib.datasource.DataClassMatrixDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.utils.HdfTypeUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Hello world!
 *
 */
public class HdfFixedPointApp {
    public static void main(String[] args) {
        new HdfFixedPointApp().run();
    }
    private void run() {
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = HdfCompoundApp.class.getResource("/singleint.h5").getFile();
            try(FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                tryScalarDataSpliterator(channel, reader);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        tryHdfApiCompound();
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = HdfFixedPointApp.class.getResource("/randomints.h5").getFile();
            try(FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                tryTemperatureSpliterator(channel, reader);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        tryHdfApiInts();
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = HdfFixedPointApp.class.getResource("/weather_data.h5").getFile();
            try(FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                tryWeatherSpliterator(channel, reader);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        tryHdfApiInts();
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

    @Data
    public static class Scalar {
        private BigInteger data;
    }

    private void tryScalarDataSpliterator(FileChannel fileChannel, HdfFileReader reader) throws IOException {
        DataClassDataSource<HdfFixedPoint> dataSource = new DataClassDataSource<>(reader.getDataObjectHeaderPrefix(), 0, fileChannel, reader.getDataAddress(), HdfFixedPoint.class);
        HdfFixedPoint[] allData = dataSource.readAll();
        System.out.println("Scalar readAll stats = " + Arrays.stream(allData).map(HdfFixedPoint::toBigInteger).collect(Collectors.summarizingInt(BigInteger::intValue)));
        System.out.println("Scalar streaming list = " + dataSource.stream().map(fp->HdfTypeUtils.populateFromFixedPoint(Scalar.class, "data", fp, 0)).toList());
        System.out.println("Scalar parallelStreaming list = " + dataSource.parallelStream().map(fp->HdfTypeUtils.populateFromFixedPoint(Scalar.class, "data", fp, 0)).toList());
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TemperatureData {
        private BigInteger temperature;
    }

    public void tryTemperatureSpliterator(FileChannel fileChannel, HdfFileReader reader) throws IOException {
        DataClassDataSource<HdfFixedPoint> dataSource = new DataClassDataSource<>(reader.getDataObjectHeaderPrefix(), 0, fileChannel, reader.getDataAddress(), HdfFixedPoint.class);
        HdfFixedPoint[] allData = dataSource.readAll();
        System.out.println("Vector readAll stats  = " + Arrays.stream(allData).map(HdfFixedPoint::toBigInteger).collect(Collectors.summarizingInt(BigInteger::intValue)));
        System.out.println("Vector streaming stats = " + dataSource.stream()
                .map(fp->HdfTypeUtils.populateFromFixedPoint(TemperatureData.class, "temperature", fp, 0))
                .map(TemperatureData::getTemperature)
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
        System.out.println("Vector parallel streaming stats = " + dataSource.parallelStream()
                .map(fp->HdfTypeUtils.populateFromFixedPoint(TemperatureData.class, "temperature", fp, 0))
                .map(TemperatureData::getTemperature)
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
    }

    private void tryWeatherSpliterator(FileChannel fileChannel, HdfFileReader reader) throws IOException {
        DataClassMatrixDataSource<HdfFixedPoint> dataSource = new DataClassMatrixDataSource<>(reader.getDataObjectHeaderPrefix(), 2, fileChannel, reader.getDataAddress(), HdfFixedPoint.class);
        HdfFixedPoint[][] allData = dataSource.readAll();
        // Print the matrix values
        System.out.println("Matrix readAll() = ");
        for (int i = 0; i < allData.length; i++) {
            for (int j = 0; j < allData[i].length; j++) {
                System.out.print(allData[i][j].toBigDecimal(2) + " ");
            }
            System.out.println(); // New line after each row
        }

        Stream<HdfFixedPoint[]> stream = dataSource.stream();
        // Print all values
        System.out.println("Matrix stream() = ");
        stream.forEach(array -> {
            for (HdfFixedPoint value : array) {
                System.out.print(value.toBigDecimal(2) + " ");
            }
            System.out.println(); // Newline after each array
        });
        Stream<HdfFixedPoint[]> parallelStream = dataSource.parallelStream();

        // Print all values in order
        System.out.println("Matrix parallelStream() = ");
        parallelStream.forEachOrdered(array -> {
            for (HdfFixedPoint value : array) {
                System.out.print(value.toBigDecimal(2) + " ");
            }
            System.out.println();
        });
    }

}
