package org.hdf5javalib.examples.write;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Hello world!
 *
 */
public class HdfFixedPointWrite {
    public static void main(String[] args) throws IOException {
        new HdfFixedPointWrite().run();
    }

    private void run() {
        tryHdfApiScalar();
        tryHdfApiInts("vector_each.h5", this::writeEach);
        tryHdfApiInts("vector_all.h5", this::writeAll);
        tryHdfApiMatrixInts("weatherdata_each.h5", this::writeEachMatrix);
        tryHdfApiMatrixInts("weatherdata_all.h5", this::writeAllMatrix);
    }

    private void tryHdfApiMatrixInts(String FILE_NAME, Consumer<MatrixWriterParams> writer) {
        String filePath = "/weatherdata.csv";

        List<String> labels;
        List<List<BigDecimal>> data = new ArrayList<>();
        try (Reader reader = new InputStreamReader(Objects.requireNonNull(HdfFixedPointWrite.class.getResourceAsStream(filePath)), StandardCharsets.UTF_8)) {
            // Updated approach to set header and skip the first record
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build();

            CSVParser parser = new CSVParser(reader, csvFormat);

            // Get column labels
            labels = new ArrayList<>(parser.getHeaderNames());
//            System.out.println("Labels: " + labels);

            // Read rows and convert values to BigDecimal
            for (CSVRecord record : parser) {
                List<BigDecimal> row = new ArrayList<>();
                for (String label : labels) {
                    String value = record.get(label);
                    if (value != null && !value.isEmpty()) {
                        row.add(new BigDecimal(value).setScale(2, RoundingMode.HALF_UP));
                    } else {
                        row.add(null); // Handle missing values
                    }
                }
                data.add(row);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        try (SeekableByteChannel channel = Files.newByteChannel(new File(FILE_NAME).toPath(), FILE_OPTIONS)) {
            final String DATASET_NAME = "weatherdata";

            final int NUM_RECORDS = 4;
            final int NUM_DATAPOINTS = 17;

            // Create a new HDF5 file
            HdfFile file = new HdfFile(channel);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {
                    HdfWriteUtils.hdfFixedPointFromValue(NUM_RECORDS, file.getFixedPointDatatypeForLength()),
                    HdfWriteUtils.hdfFixedPointFromValue(NUM_DATAPOINTS, file.getFixedPointDatatypeForLength())
            };

            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 2, DataspaceMessage.buildFlagSet(true, false), hdfDimensions, hdfDimensions, false, (byte)0, computeDataSpaceMessageSize(hdfDimensions));

            FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField( false, false, false, false),
                    (short)4, (short)7, (short)25);

            HdfDataSet dataset = file.createDataSet(DATASET_NAME, fixedPointDatatype, dataSpaceMessage);

            writer.accept(new MatrixWriterParams(NUM_RECORDS, NUM_DATAPOINTS, fixedPointDatatype, dataset, data));

            dataset.close();
            file.close();

            // auto close

            System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static short computeDataSpaceMessageSize(HdfFixedPoint[] hdfDimensions) {
        short dataSpaceMessageSize = 8;
        if ( hdfDimensions != null ) {
            for (HdfFixedPoint dimension : hdfDimensions) {
                dataSpaceMessageSize += (short) dimension.getDatatype().getSize();
            }
        }
        if ( hdfDimensions != null ) {
            for (HdfFixedPoint maxDimension : hdfDimensions) {
                dataSpaceMessageSize += (short) maxDimension.getDatatype().getSize();
            }
        }
        return dataSpaceMessageSize;
    }

    private void tryHdfApiScalar() {
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "FixedPointValue";

        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(Paths.get("scalar.h5"), FILE_OPTIONS)) {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(seekableByteChannel);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, (short) 8);

            FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField( false, false, false, true),
                    (short)4, (short)0, (short)32);

            // Create dataset
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, fixedPointDatatype, dataSpaceMessage);

            HdfDisplayUtils.writeVersionAttribute(file, dataset);

            ByteBuffer byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, new HdfFixedPoint(BigInteger.valueOf(42), fixedPointDatatype));
            byteBuffer.flip();
            // Write to dataset
            dataset.write(byteBuffer);

            dataset.close();
            file.close();

            // auto close
            System.out.println("HDF5 file scalar.h5 created and written successfully!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void tryHdfApiInts(String FILE_NAME, Consumer<WriterParams> writer) {
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "vector";
        final int NUM_RECORDS = 1000;

        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(Paths.get(FILE_NAME), FILE_OPTIONS)) {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(seekableByteChannel);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {HdfWriteUtils.hdfFixedPointFromValue(NUM_RECORDS, file.getFixedPointDatatypeForLength())};

            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, DataspaceMessage.buildFlagSet(true, false), hdfDimensions, hdfDimensions, false, (byte)0, computeDataSpaceMessageSize(hdfDimensions));

            FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField( false, false, false, true),
                    (short)8, (short)0, (short)64);

            // Create dataset
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, fixedPointDatatype, dataSpaceMessage);

            HdfDisplayUtils.writeVersionAttribute(file, dataset);

            writer.accept(new WriterParams(NUM_RECORDS, fixedPointDatatype, dataset));

            dataset.close();
            file.close();

            // auto close
            System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SneakyThrows
    private void writeEachMatrix(MatrixWriterParams writerParams) {
        AtomicInteger countHolder = new AtomicInteger(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.fixedPointDatatype.getSize() * writerParams.NUM_DATAPOINTS).order(writerParams.fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        BigDecimal twoShifted = new BigDecimal(BigInteger.ONE.shiftLeft(writerParams.fixedPointDatatype.getBitOffset()));
        BigDecimal point5 = new BigDecimal("0.5");
        // Write to dataset
        writerParams.dataset.write(() -> {
            int count = countHolder.getAndIncrement();
            if (count >= writerParams.NUM_RECORDS) return ByteBuffer.allocate(0);
            byteBuffer.clear();
            makeBitOffsetValues(writerParams, byteBuffer, twoShifted, point5, count);
            byteBuffer.flip();
            return byteBuffer;
        });
    }

    @SneakyThrows
    private void writeAllMatrix(MatrixWriterParams writerParams) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.fixedPointDatatype.getSize() * writerParams.NUM_DATAPOINTS * writerParams.NUM_RECORDS).order(writerParams.fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        BigDecimal twoShifted = new BigDecimal(BigInteger.ONE.shiftLeft(writerParams.fixedPointDatatype.getBitOffset()));
        BigDecimal point5 = new BigDecimal("0.5");
        for(int r=0; r<writerParams.NUM_RECORDS; r++) {
            makeBitOffsetValues(writerParams, byteBuffer, twoShifted, point5, r);
        }
        byteBuffer.flip();
        // Write to dataset
        writerParams.dataset.write(byteBuffer);
    }

    private void makeBitOffsetValues(MatrixWriterParams writerParams, ByteBuffer byteBuffer, BigDecimal twoShifted, BigDecimal point5, int r) {
        for(int c=0; c < writerParams.NUM_DATAPOINTS; ++c ) {
            BigDecimal rawValue = writerParams.values.get(r).get(c).multiply(twoShifted).add(point5);
            BigInteger rawValueShifted = rawValue.toBigInteger();
            new HdfFixedPoint(rawValueShifted, writerParams.fixedPointDatatype)
                    .writeValueToByteBuffer(byteBuffer);
        }
    }

    @SneakyThrows
    private void writeEach(WriterParams writerParams) {
        AtomicInteger countHolder = new AtomicInteger(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.fixedPointDatatype.getSize()).order(writerParams.fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        // Write to dataset
        writerParams.dataset.write(() -> {
            int count = countHolder.getAndIncrement();
            if (count >= writerParams.NUM_RECORDS) return ByteBuffer.allocate(0);
            byteBuffer.clear();
//            HdfWriteUtils.writeBigIntegerAsHdfFixedPoint(BigInteger.valueOf((long) (Math.random() *40.0 + 10.0)), writerParams.fixedPointDatatype, byteBuffer);
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, new HdfFixedPoint(BigInteger.valueOf((long)(Math.random() *40.0 + 10.0)), writerParams.fixedPointDatatype));
            byteBuffer.flip();
            return byteBuffer;
        });
    }

    @SneakyThrows
    private void writeAll(WriterParams writerParams) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.fixedPointDatatype.getSize() * writerParams.NUM_RECORDS).order(writerParams.fixedPointDatatype.isBigEndian() ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        for(int i=0; i<writerParams.NUM_RECORDS; i++) {
//            HdfWriteUtils.writeBigIntegerAsHdfFixedPoint(BigInteger.valueOf((long) (Math.random() *40.0 + 10.0)), writerParams.fixedPointDatatype, byteBuffer);
//            HdfWriteUtils.writeBigIntegerAsHdfFixedPoint(, writerParams.fixedPointDatatype, byteBuffer);
            HdfWriteUtils.writeFixedPointToBuffer(byteBuffer, new HdfFixedPoint(BigInteger.valueOf(i+1), writerParams.fixedPointDatatype));
        }
        byteBuffer.flip();
        // Write to dataset
        writerParams.dataset.write(byteBuffer);
    }

    @AllArgsConstructor
    static class WriterParams {
        int NUM_RECORDS;
        FixedPointDatatype fixedPointDatatype;
        HdfDataSet dataset;
    }

    @AllArgsConstructor
    static class MatrixWriterParams {
        int NUM_RECORDS;
        int NUM_DATAPOINTS;
        FixedPointDatatype fixedPointDatatype;
        HdfDataSet dataset;
        List<List<BigDecimal>> values;
    }
}
