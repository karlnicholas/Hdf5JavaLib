package org.hdf5javalib.examples.read;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.examples.write.HdfFixedPointApp;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.*;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hdf5javalib.file.dataobject.message.datatype.FloatingPointDatatype.ClassBitField.MantissaNormalization.IMPLIED_SET;
import static org.hdf5javalib.utils.HdfDisplayUtils.writeVersionAttribute;

/**
 * Hello world!
 */
@Slf4j
public class HdfCompoundApp {
    public static void main(String[] args) {
        new HdfCompoundApp().run();
    }

    private void run() {
//        try {
//            Path filePath = getResourcePath("compound_example.h5");
//            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
//                HdfFileReader reader = new HdfFileReader(channel).readFile();
//                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("CompoundData") ) {
//                    displayData(channel, dataSet, reader);
//                }
////                reader.getGlobalHeap().printDebug();
//            }
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        tryHdfApiCompound();
    }
    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    public void tryHdfApiCompound() {
        final String FILE_NAME = "compound_example.h5";
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "CompoundData";
        final int NUM_RECORDS = 1000;

        try (SeekableByteChannel seekableByteChannel = Files.newByteChannel(Paths.get(FILE_NAME), FILE_OPTIONS)) {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(seekableByteChannel);

            FixedPointDatatype attributeType = new FixedPointDatatype(FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField(false, false, false, false),
                    1, (short) 0, (short) 8);
            VariableLengthDatatype variableLengthDatatype = new VariableLengthDatatype(VariableLengthDatatype.createClassAndVersion(),
                    VariableLengthDatatype.createClassBitField(VariableLengthDatatype.Type.STRING, VariableLengthDatatype.PaddingType.NULL_TERMINATE, VariableLengthDatatype.CharacterSet.ASCII),
                    (short) 16, attributeType);
            variableLengthDatatype.setGlobalHeap(file.getGlobalHeap());

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
                            variableLengthDatatype),
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
                    new CompoundMemberDatatype("scaledUintVal", 88, 0, 0, new int[4],
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
            HdfFixedPoint[] hdfDimensions = {HdfWriteUtils.hdfFixedPointFromValue(NUM_RECORDS, file.getFixedPointDatatypeForLength())};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointApp.computeDataSpaceMessageSize(hdfDimensions));

            // Create dataset
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, compoundType, dataSpaceMessage);
            file.getFileAllocation().printBlocks();

            // ADD ATTRIBUTE: "GIT root revision"
            writeVersionAttribute(file, dataset);
            file.getFileAllocation().printBlocks();
            writeCompoundAll(dataset, file);
//            AtomicInteger countHolder = new AtomicInteger(0);
//            ByteBuffer buffer = ByteBuffer.allocate(compoundType.getSize()).order(ByteOrder.LITTLE_ENDIAN);
//            // Write to dataset
//            dataset.write(() -> {
//                int count = countHolder.getAndIncrement();
//                if (count >= NUM_RECORDS) return ByteBuffer.allocate(0);
//                CompoundExample instance = CompoundExample.builder()
//                        .recordId(count + 1000L)
//                        .fixedStr("FixedData")
//                        .varStr("varStr:" + (count + 1))
//                        .floatVal(((float)count)*3.14F)
//                        .doubleVal(((double)count)*2.718D)
//                        .int8_Val(getCycledInt8(count))
//                        .uint8_Val(getCycledUint8(count))
//                        .int16_Val(getCycledInt16(count))
//                        .uint16_Val(getCycledUint16(count))
//                        .int32_Val(getCycledInt32(count))
//                        .uint32_Val(getCycledUint32(count))
//                        .int64_Val(getCycledInt64(count))
//                        .uint64_Val(getCycledUint64(count))
//                        .scaledUintVal(BigDecimal.valueOf(count + 1).add(BigDecimal.valueOf((count % 4) * 0.25)))
//                        .build();
//                buffer.clear();
//                HdfWriteUtils.writeCompoundTypeToBuffer(instance, compoundType, buffer, CompoundExample.class);
//                buffer.position(0);
//                return buffer;
//            });

            file.getFileAllocation().printBlocks();
            dataset.close();
            file.getFileAllocation().printBlocks();
            file.close();
            file.getFileAllocation().printBlocks();

            // auto close

            System.out.println("HDF5 file created and written successfully!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @SneakyThrows
    private static void writeCompoundAll(HdfDataSet dataset, HdfDataFile hdfDataFile) {
        HdfFixedPoint[] dimensionSizes= dataset.getdimensionSizes();
        hdfDataFile.getFileAllocation().allocateAndSetDataBlock(dataset.getDatasetName(), dimensionSizes[0].getInstance(Long.class));
        boolean requiresGlobalHeap = dataset.getHdfDatatype().requiresGlobalHeap(false);
        if (requiresGlobalHeap) {
            if (!hdfDataFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfDataFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
        }

        int numRecords = 1000;
        CompoundDatatype compoundType = (CompoundDatatype) dataset.getHdfDatatype();
        int bufferSize = numRecords * compoundType.getSize();
        ByteBuffer fileBuffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer byteBuffer = ByteBuffer.allocate(compoundType.getSize()).order(ByteOrder.LITTLE_ENDIAN);
        for (int count = 0; count < numRecords; count++) {
            CompoundExample instance = buildCompoundExample(count);
            byteBuffer.clear();
            HdfWriteUtils.writeCompoundTypeToBuffer(instance, compoundType, byteBuffer, CompoundExample.class);
            byteBuffer.rewind();
            fileBuffer.put(byteBuffer);
        }
        fileBuffer.rewind();
        dataset.write(fileBuffer);
    }

    @SneakyThrows
    private static void writeCompoundEach(HdfDataSet dataset, HdfDataFile hdfDataFile) {

        int numRecords = 1000;
        CompoundDatatype compoundType = (CompoundDatatype) dataset.getHdfDatatype();
        int bufferSize = compoundType.getSize();
        AtomicInteger countHolder = new AtomicInteger(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize).order(ByteOrder.LITTLE_ENDIAN);
        dataset.write(() -> {
            int count = countHolder.getAndIncrement();
            if (count >= numRecords) return ByteBuffer.allocate(0);
            CompoundExample instance = buildCompoundExample(count);
            byteBuffer.clear();
            HdfWriteUtils.writeCompoundTypeToBuffer(instance, compoundType, byteBuffer, CompoundExample.class);
            byteBuffer.flip();
            return byteBuffer;
        });
    }

    private static CompoundExample buildCompoundExample(int count) {
        return CompoundExample.builder()
                .recordId(count + 1000L)
                .fixedStr("FixedData")
                .varStr("varStr:" + (count + 1))
                .floatVal(((float) count) * 3.14F)
                .doubleVal(((double) count) * 2.718D)
                .int8_Val(getCycledInt8(count))
                .uint8_Val(getCycledUint8(count))
                .int16_Val(getCycledInt16(count))
                .uint16_Val(getCycledUint16(count))
                .int32_Val(getCycledInt32(count))
                .uint32_Val(getCycledUint32(count))
                .int64_Val(getCycledInt64(count))
                .uint64_Val(getCycledUint64(count))
                .scaledUintVal(BigDecimal.valueOf(count + 1).add(BigDecimal.valueOf((count % 4) * 0.25)))
                .build();
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
        private BigDecimal scaledUintVal;
    }

    @Data
    public static class MonitoringData {
        private String siteName;
        private Float airQualityIndex;
        private Double temperature;
        private Integer sampleCount;
    }


    public void displayData(SeekableByteChannel seekableByteChannel, HdfDataSet dataSet, HdfDataFile hdfDataFile) throws IOException {
//        System.out.println("Count = " + new TypedDataSource<>(dataSet, fileChannel, HdfCompound.class).streamVector().count());

        System.out.println("Ten Rows:");
        new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, HdfCompound.class)
                .streamVector()
                .limit(10)
                .forEach(c -> System.out.println("Row: " + c.getMembers()));

        System.out.println("Ten BigDecimals = " + new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, HdfCompound.class).streamVector()
                        .filter(c-> c.getMembers().get(0).getInstance(Long.class) < 1010 )
                .map(c->c.getMembers().get(13).getInstance(BigDecimal.class)).toList());

        System.out.println("RecordId < 1010, custom class:");
        new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, CompoundExample.class)
                .streamVector()
                .filter(c -> c.getRecordId() < 1010)
                .forEach(c -> System.out.println("Row: " + c));
        System.out.println("DONE");
    }

    public void tryCompoundSpliterator(FileChannel fileChannel, HdfDataSet dataSet, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<MonitoringData> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, MonitoringData.class);
        MonitoringData[] allData = dataSource.readVector();
        System.out.println("*** readAll: \r\n" + Arrays.asList(allData).stream().map(Object::toString).collect(Collectors.joining("\n")));
        System.out.println("*** stream: \r\n" + dataSource.streamVector().map(Object::toString).collect(Collectors.joining("\n")));
        System.out.println("\"*** parallelStream: \r\n" + dataSource.parallelStreamVector().map(Object::toString).collect(Collectors.joining("\n")));

        new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, HdfCompound.class).streamVector().forEach(System.out::println);
        new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, String.class).streamVector().forEach(System.out::println);
    }


    private void tryCustomSpliterator(FileChannel fileChannel, HdfDataSet dataSet, HdfDataFile hdfDataFile) {
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
        new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, MonitoringData.class).streamVector().forEach(System.out::println);
    }

    private static final int CYCLE_LENGTH = 5;

    // --- Signed Types ---

    public static byte getCycledInt8(int index) {
        int cycleIndex = index % CYCLE_LENGTH;
        return switch (cycleIndex) {
            case 0 -> Byte.MIN_VALUE;                            // -128 (0x80)
            case 1 -> (byte) (-(Byte.MAX_VALUE / 2) - 1);        //  -64 (0xC0) approx
            case 2 -> 0;                                         //    0 (0x00)
            case 3 -> (byte) (Byte.MAX_VALUE / 2);               //   63 (0x3F) approx
            default -> Byte.MAX_VALUE;                   //  127 (0x7F)
        };
    }

    public static short getCycledInt16(int index) {
        int cycleIndex = index % CYCLE_LENGTH;
        return switch (cycleIndex) {
            case 0 -> Short.MIN_VALUE;
            case 1 -> (short) (-(Short.MAX_VALUE / 2) - 1);
            case 2 -> 0;
            case 3 -> (short) (Short.MAX_VALUE / 2);
            default -> Short.MAX_VALUE;
        };
    }

    public static int getCycledInt32(int index) {
        int cycleIndex = index % CYCLE_LENGTH;
        return switch (cycleIndex) {
            case 0 -> Integer.MIN_VALUE;
            case 1 -> -(Integer.MAX_VALUE / 2) - 1;
            case 2 -> 0;
            case 3 -> Integer.MAX_VALUE / 2;
            default -> Integer.MAX_VALUE;
        };
    }

    public static long getCycledInt64(int index) {
        int cycleIndex = index % CYCLE_LENGTH;
        return switch (cycleIndex) {
            case 0 -> Long.MIN_VALUE;
            case 1 -> -(Long.MAX_VALUE / 2) - 1;
            case 2 -> 0L;
            case 3 -> Long.MAX_VALUE / 2;
            default -> Long.MAX_VALUE;
        };
    }

    // --- Unsigned Types (Return next larger signed type to hold value) ---
    // --- Or return 'long' for uint64 and handle bit pattern ---

    public static short getCycledUint8(int index) { // Returns short to hold 0-255
        int cycleIndex = index % CYCLE_LENGTH;
        return switch (cycleIndex) {
            case 0 -> 0;                     // 0x00
            case 1 -> 255 / 4;               // 63 (0x3F) approx
            case 2 -> 255 / 2;               // 127 (0x7F) approx
            case 3 -> (255 / 4) * 3;         // 189 (0xBD) approx
            default -> 255;          // 255 (0xFF)
        };
    }

    public static int getCycledUint16(int index) { // Returns int to hold 0-65535
        int cycleIndex = index % CYCLE_LENGTH;
        int max_val = 65535; // 0xFFFF
        return switch (cycleIndex) {
            case 0 -> 0;
            case 1 -> max_val / 4;
            case 2 -> max_val / 2;
            case 3 -> (max_val / 4) * 3;
            default -> max_val;
        };
    }

    public static long getCycledUint32(int index) { // Returns long to hold 0-(2^32-1)
        int cycleIndex = index % CYCLE_LENGTH;
        long max_val = 0xFFFFFFFFL; // (1L << 32) - 1;
        return switch (cycleIndex) {
            case 0 -> 0L;
            case 1 -> max_val / 4L;
            case 2 -> max_val / 2L;
            case 3 -> (max_val / 4L) * 3L;
            default -> max_val;
        };
    }

    // For uint64, we can return long and rely on the bit pattern being correct,
    // or use BigInteger if the HDF5 library specifically needs that. Assuming primitive:
    public static BigInteger getCycledUint64(int index) { // Returns long, bit pattern matches uint64
        int cycleIndex = index % CYCLE_LENGTH;
        // Use BigInteger for calculation constants to avoid signed long issues
        BigInteger MAX_U64 = new BigInteger("18446744073709551615"); // 2^64 - 1
        BigInteger FOUR = BigInteger.valueOf(4);
        BigInteger TWO = BigInteger.valueOf(2);
        BigInteger THREE = BigInteger.valueOf(3);

        return switch (cycleIndex) {
            case 0 -> BigInteger.ZERO;
            case 1 -> MAX_U64.divide(FOUR);
            case 2 -> MAX_U64.divide(TWO);
            case 3 -> MAX_U64.divide(FOUR).multiply(THREE);
            default -> MAX_U64; // Max unsigned 64 bit is -1L signed
        };
    }
}
