package org.hdf5javalib.examples;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.*;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hdf5javalib.file.dataobject.message.datatype.FloatingPointDatatype.ClassBitField.MantissaNormalization.IMPLIED_SET;

/**
 * Hello world!
 */
@Slf4j
public class HdfCompoundApp {
    public static void main(String[] args) {
        new HdfCompoundApp().run();
    }

    private void run() {
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = HdfCompoundApp.class.getResource("/compound_example.h5").getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                try ( HdfDataSet dataSet = reader.findDataset("data", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        try {
//            HdfFileReader reader = new HdfFileReader();
//            String filePath = HdfCompoundApp.class.getResource("/compound_example_new.h5").getFile();
//            try (FileInputStream fis = new FileInputStream(filePath)) {
//                FileChannel channel = fis.getChannel();
//                reader.readFile(channel);
//                tryCompoundTestSpliterator(channel, reader);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        try {
//            HdfFileReader reader = new HdfFileReader();
//            String filePath = HdfCompoundApp.class.getResource("/env_monitoring_labels.h5").getFile();
//            try (FileInputStream fis = new FileInputStream(filePath)) {
//                FileChannel channel = fis.getChannel();
//                reader.readFile(channel);
//                tryCompoundSpliterator(channel, reader);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        try {
//            HdfFileReader reader = new HdfFileReader();
//            String filePath = HdfCompoundApp.class.getResource("/env_monitoring.h5").getFile();
//            try (FileInputStream fis = new FileInputStream(filePath)) {
//                FileChannel channel = fis.getChannel();
//                reader.readFile(channel);
//                tryCustomSpliterator(channel, reader);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        tryHdfApiCompound();
    }

    private void writeVersionAttribute(HdfDataSet dataset) {
        String ATTRIBUTE_NAME = "GIT root revision";
        String ATTRIBUTE_VALUE = "Revision: , URL: ";
        BitSet classBitField = StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII);
        // value
        StringDatatype attributeType = new StringDatatype(StringDatatype.createClassAndVersion(), classBitField, (short) ATTRIBUTE_VALUE.length());
        // data type, String, DATASET_NAME.length
        short dataTypeMessageSize = 8;
        dataTypeMessageSize += attributeType.getSizeMessageData();
        // to 8 byte boundary
        dataTypeMessageSize += ((dataTypeMessageSize + 7) & ~7);
        DatatypeMessage dt = new DatatypeMessage(attributeType, (byte)1, dataTypeMessageSize);
        HdfFixedPoint[] hdfDimensions = {};
        // scalar, 1 string
        short dataspaceMessageSize = 8;
        DataspaceMessage ds = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false),
                hdfDimensions, hdfDimensions, false, (byte)0, dataspaceMessageSize);
        HdfString hdfString = new HdfString(ATTRIBUTE_VALUE.getBytes(), attributeType);
        dataset.createAttribute(ATTRIBUTE_NAME, dt, ds, hdfString);
    }

    public void tryHdfApiCompound() {
        final String FILE_NAME = "compound_example.h5";
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "CompoundData";
        final int NUM_RECORDS = 1000;

        try {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(FILE_NAME, FILE_OPTIONS);

            List<CompoundMemberDatatype> compoundData = List.of(
                    new CompoundMemberDatatype("recordId", 0, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, false),
                                    (short) 8, (short) 0, (short) 64)),
                    new CompoundMemberDatatype("fixedStr", 8, 0, 0, new int[4],
                            new StringDatatype(
                                    StringDatatype.createClassAndVersion(),
                                    StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII),
                                    (short) 10)),
                    new CompoundMemberDatatype("varStr", 24, 0, 0, new int[4],
                            new VariableLengthDatatype(
                                    VariableLengthDatatype.createClassAndVersion(),
                                    VariableLengthDatatype.createClassBitField(VariableLengthDatatype.PaddingType.NULL_TERMINATE, VariableLengthDatatype.CharacterSet.ASCII),
                                    (short) 16,
                                    0)),
                    new CompoundMemberDatatype("floatVal", 40, 0, 0, new int[4],
                            new FloatingPointDatatype(
                                    FloatingPointDatatype.createClassAndVersion(),
                                    FloatingPointDatatype.ClassBitField.createBitSet(FloatingPointDatatype.ClassBitField.ByteOrder.LITTLE_ENDIAN, false, false, false, IMPLIED_SET, 31),
                                    4, (short) 0, (short) 32, (byte) 23, (byte) 8, (byte) 0, (byte) 23, 127)),
                    new CompoundMemberDatatype("doubleVal", 48, 0, 0, new int[4],
                            new FloatingPointDatatype(
                                    FloatingPointDatatype.createClassAndVersion(),
                                    FloatingPointDatatype.ClassBitField.createBitSet(FloatingPointDatatype.ClassBitField.ByteOrder.LITTLE_ENDIAN, false, false, false, IMPLIED_SET, 63),
                                    8, (short) 0, (short) 64, (byte) 52, (byte) 11, (byte) 0, (byte) 52, 1023)),
                    new CompoundMemberDatatype("int8_Val", 56, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, true),
                                    (short) 1, (short) 0, (short) 8)),
                    new CompoundMemberDatatype("uint8_Val", 57, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, false),
                                    (short) 1, (short) 0, (short) 8)),
                    new CompoundMemberDatatype("int16_Val", 58, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, true),
                                    (short) 2, (short) 0, (short) 16)),
                    new CompoundMemberDatatype("uint16_Val", 60, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, false),
                                    (short) 2, (short) 0, (short) 16)),
                    new CompoundMemberDatatype("int32_Val", 64, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, true),
                                    (short) 4, (short) 0, (short) 32)),
                    new CompoundMemberDatatype("uint32_Val", 68, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, false),
                                    (short) 4, (short) 0, (short) 32)),
                    new CompoundMemberDatatype("int64_Val", 72, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, true),
                                    (short) 8, (short) 0, (short) 64)),
                    new CompoundMemberDatatype("uint64_Val", 80, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, false),
                                    (short) 8, (short) 0, (short) 64)),
                    new CompoundMemberDatatype("bitfieldVal", 88, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField(false, false, false, false),
                                    (short) 8, (short) 7, (short) 57))
            );
            short compoundSize = (short) compoundData.stream().mapToInt(c -> c.getType().getSize()).sum();
            // for varLen string.
//            compoundSize += (40 - 16) + 6 + 4 + 2;
            compoundSize += 12;

            // Define Compound DataType correctly
            CompoundDatatype compoundType = new CompoundDatatype(
                    CompoundDatatype.createClassAndVersion(),
                    CompoundDatatype.createClassBitField((short) compoundData.size()),
                    compoundSize, compoundData);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(NUM_RECORDS)};
            short dataspaceMessageSize = 8;
            if ( hdfDimensions != null ) {
                for (HdfFixedPoint dimension : hdfDimensions) {
                    dataspaceMessageSize += dimension.getDatatype().getSize();
                }
            }
            if ( hdfDimensions != null ) {
                for (HdfFixedPoint maxDimension : hdfDimensions) {
                    dataspaceMessageSize += maxDimension.getDatatype().getSize();
                }
            }
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false), hdfDimensions, hdfDimensions, false, (byte)0, dataspaceMessageSize);

            // Create dataset
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, compoundType, dataSpaceMessage);

            // ADD ATTRIBUTE: "GIT root revision"
            writeVersionAttribute(dataset);

            AtomicInteger countHolder = new AtomicInteger(0);
            ByteBuffer buffer = ByteBuffer.allocate(compoundType.getSize()).order(ByteOrder.LITTLE_ENDIAN);
            // Write to dataset
            dataset.write(() -> {
                int count = countHolder.getAndIncrement();
                if (count >= NUM_RECORDS) return ByteBuffer.allocate(0);
                CompoundExample instance = CompoundExample.builder()
                        .recordId(count + 1000L)
                        .fixedStr("FixedData")
                        .varStr("varData:" + (int)(Math.random() * 1900))
                        .floatVal(3.14F)
                        .doubleVal(2.718D)
                        .int8_Val(int8_Val(count))
                        .uint8_Val(uint8_Val(count))
                        .int16_Val(int16_Val(count))
                        .uint16_Val(uint16_Val(count))
                        .int32_Val(int32_Val(count))
                        .uint32_Val(uint32_Val(count))
                        .int64_Val(int64_Val(count))
                        .uint64_Val(uint64_Val(count))
                        .bitfieldVal(BigDecimal.valueOf(count + 1).add(BigDecimal.valueOf((count % 4) * 0.25)))
                        .build();
                buffer.clear();
                HdfWriteUtils.writeCompoundTypeToBuffer(instance, compoundType, buffer, CompoundExample.class);
                buffer.position(0);
                return buffer;
            });

            dataset.close();
            file.close();

            // auto close

            System.out.println("HDF5 file created and written successfully!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CompoundExample {
        private Long recordId;
        private String fixedStr;
        private String varStr;
        private Float floatVal;
        private Double doubleVal;
        private Byte int8_Val;
        private Short int16_Val;
        private Integer int32_Val;
        private Long int64_Val;
        private Short uint8_Val;
        private Integer uint16_Val;
        private Long uint32_Val;
        private BigInteger uint64_Val;
        private BigDecimal bitfieldVal;
    }

    @Data
    public static class MonitoringData {
        private String siteName;
        private Float airQualityIndex;
        private Double temperature;
        private Integer sampleCount;
    }


    public void displayData(FileChannel fileChannel, HdfDataSet dataSet) throws IOException {
        System.out.println("Count = " + new TypedDataSource<>(dataSet, fileChannel, HdfCompound.class).streamVector().count());

        System.out.println("Ten Rows:");
        new TypedDataSource<>(dataSet, fileChannel, HdfCompound.class)
                .streamVector()
                .limit(10)
                .forEach(c -> System.out.println("Row: " + c.getMembers()));

        System.out.println("Ten BigDecimals = " + new TypedDataSource<>(dataSet, fileChannel, HdfCompound.class).streamVector()
                        .filter(c->c.getMembers().get(0).getInstance(Long.class).longValue() < 1010 )
                .map(c->c.getMembers().get(13).getInstance(BigDecimal.class)).toList());

        System.out.println("Ten Rows:");
        new TypedDataSource<>(dataSet, fileChannel, CompoundExample.class)
                .streamVector()
                .filter(c -> c.getRecordId() < 1010)
                .forEach(c -> System.out.println("Row: " + c));
    }

    public void tryCompoundSpliterator(FileChannel fileChannel, HdfDataSet dataSet) throws IOException {
        TypedDataSource<MonitoringData> dataSource = new TypedDataSource<>(dataSet, fileChannel, MonitoringData.class);
        MonitoringData[] allData = dataSource.readVector();
        System.out.println("*** readAll: \r\n" + Arrays.asList(allData).stream().map(Object::toString).collect(Collectors.joining("\n")));
        System.out.println("*** stream: \r\n" + dataSource.streamVector().map(Object::toString).collect(Collectors.joining("\n")));
        System.out.println("\"*** parallelStream: \r\n" + dataSource.parallelStreamVector().map(Object::toString).collect(Collectors.joining("\n")));

        new TypedDataSource<>(dataSet, fileChannel, HdfCompound.class).streamVector().forEach(System.out::println);
        new TypedDataSource<>(dataSet, fileChannel, String.class).streamVector().forEach(System.out::println);
    }


    private void tryCustomSpliterator(FileChannel fileChannel, HdfDataSet dataSet) {
        CompoundDatatype.addConverter(MonitoringData.class, (bytes, datatype) -> {
            MonitoringData monitoringData = new MonitoringData();
            datatype.getMembers().forEach(member -> {
                byte[] memberBytes = Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset() + member.getSize());
                switch (member.getName()) {
                    case "Site Name" -> monitoringData.setSiteName(member.getInstance(String.class, memberBytes));
                    case "Air Quality Index" -> monitoringData.setAirQualityIndex(member.getInstance(Float.class, memberBytes));
                    case "Temperature" -> monitoringData.setTemperature(member.getInstance(Double.class, memberBytes));
                    case "Sample Count" -> monitoringData.setSampleCount(member.getInstance(Integer.class, memberBytes));
                    default -> throw new RuntimeException("Error");
                }
            });
            return monitoringData;
        });
        new TypedDataSource<>(dataSet, fileChannel, MonitoringData.class).streamVector().forEach(System.out::println);
    }

    private static final int CYCLE_LENGTH = 10;

    // Signed byte: -128 to 127
    public static byte int8_Val(int index) {
        double min = Byte.MIN_VALUE;  // -128
        double max = Byte.MAX_VALUE;  // 127
        double step = (max - min) / (CYCLE_LENGTH - 1);
        double value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max : min + (index % CYCLE_LENGTH) * step;
        return (byte) value;
    }

    // Unsigned byte: 0 to 255
    public static short uint8_Val(int index) {
        int min = 0;
        int max = 255;
        int range = max - min;
        int step = range / (CYCLE_LENGTH - 1);
        int value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max : min + (index % CYCLE_LENGTH) * step;
        return (short) value; // Short to hold 0-255
    }

    // Signed short: -32768 to 32767
    public static short int16_Val(int index) {
        double min = Short.MIN_VALUE;  // -32768
        double max = Short.MAX_VALUE;  // 32767
        double step = (max - min) / (CYCLE_LENGTH - 1);
        double value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max : min + (index % CYCLE_LENGTH) * step;
        return (short) value;
    }

    // Unsigned short: 0 to 65535
    public static int uint16_Val(int index) {
        int min = 0;
        int max = 65535;
        int range = max - min;
        int step = range / (CYCLE_LENGTH - 1);
        int value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max : min + (index % CYCLE_LENGTH) * step;
        return value; // Int to hold 0-65535
    }

    // Signed int: -2147483648 to 2147483647
    public static int int32_Val(int index) {
        double min = Integer.MIN_VALUE;  // -2147483648
        double max = Integer.MAX_VALUE;  // 2147483647
        double step = (max - min) / (CYCLE_LENGTH - 1);
        double value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max : min + (index % CYCLE_LENGTH) * step;
        return (int) value;
    }

    // Unsigned int: 0 to 4294967295
    public static long uint32_Val(int index) {
        long min = 0;
        long max = 4294967295L;
        long range = max - min;
        long step = range / (CYCLE_LENGTH - 1);
        long value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max : min + (index % CYCLE_LENGTH) * step;
        return value; // Long to hold 0-4294967295
    }

    // Signed long: -9223372036854775808 to 9223372036854775807
    public static long int64_Val(int index) {
        double min = Long.MIN_VALUE;  // -9223372036854775808
        double max = Long.MAX_VALUE;  // 9223372036854775807
        double step = (max - min) / (CYCLE_LENGTH - 1);
        double value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max : min + (index % CYCLE_LENGTH) * step;
        return (long) value;
    }

    // Unsigned long: 0 to 18446744073709551615
    public static BigInteger uint64_Val(int index) {
        BigInteger min = BigInteger.ZERO;
        BigInteger max = new BigInteger("18446744073709551615");
        BigInteger range = max.subtract(min);
        BigInteger step = range.divide(BigInteger.valueOf(CYCLE_LENGTH - 1));
        BigInteger value = (index % CYCLE_LENGTH == CYCLE_LENGTH - 1) ? max :
                min.add(BigInteger.valueOf(index % CYCLE_LENGTH).multiply(step));
        return value; // BigInteger to hold 0-18446744073709551615
    }
}
