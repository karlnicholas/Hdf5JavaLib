package org.hdf5javalib.examples.write;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.*;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hdf5javalib.file.dataobject.message.datatype.FloatingPointDatatype.ClassBitField.MantissaNormalization.IMPLIED_SET;
import static org.hdf5javalib.utils.HdfDisplayUtils.writeVersionAttribute;

/**
 * Demonstrates writing a compound dataset to an HDF5 file.
 * <p>
 * The {@code HdfCompoundWrite} class is an example application that creates an HDF5 file
 * containing a compound dataset with various data types, including fixed-point, floating-point,
 * strings, and scaled integers. It defines a custom compound type, writes data using both
 * bulk and individual record methods, and adds a version attribute.
 * </p>
 */
@Slf4j
public class HdfCompoundWrite {
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HdfCompoundWrite().run();
    }

    /**
     * Executes the main logic of writing a compound dataset to an HDF5 file.
     */
    private void run() {
        tryHdfApiCompound();
    }

    /**
     * Creates and writes a compound dataset to an HDF5 file.
     * <p>
     * Defines a compound datatype with multiple fields, creates a dataset with
     * 1000 records, and writes data using both bulk and individual record methods.
     * Adds a version attribute to the dataset and manages file allocation.
     * </p>
     */
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
            compoundSize += 12;

            // Define Compound DataType correctly
            CompoundDatatype compoundType = new CompoundDatatype(
                    CompoundDatatype.createClassAndVersion(),
                    CompoundDatatype.createClassBitField((short) compoundData.size()),
                    compoundSize, compoundData);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {HdfWriteUtils.hdfFixedPointFromValue(NUM_RECORDS, file.getFixedPointDatatypeForLength())};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointWrite.computeDataSpaceMessageSize(hdfDimensions));

            // Create dataset
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, compoundType, dataSpaceMessage);
            file.getFileAllocation().printBlocks();

            // ADD ATTRIBUTE: "GIT root revision"
            writeVersionAttribute(file, dataset);
            file.getFileAllocation().printBlocks();
            writeCompoundAll(dataset, file);
            file.getFileAllocation().printBlocks();
            dataset.close();
            file.getFileAllocation().printBlocks();
            file.close();
            file.getFileAllocation().printBlocks();

            System.out.println("HDF5 file created and written successfully!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes all compound records to the dataset in bulk.
     *
     * @param dataset    the dataset to write to
     * @param hdfDataFile the HDF5 file context
     * @throws IOException if an I/O error occurs
     */
    @SneakyThrows
    private static void writeCompoundAll(HdfDataSet dataset, HdfDataFile hdfDataFile) {
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

    /**
     * Writes compound records to the dataset individually.
     *
     * @param dataset    the dataset to write to
     * @param hdfDataFile the HDF5 file context
     * @throws IOException if an I/O error occurs
     */
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

    /**
     * Builds a sample compound record for a given index.
     *
     * @param count the index for generating the record
     * @return a populated CompoundExample instance
     */
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

    /**
     * A data class representing a compound dataset record.
     */
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CompoundExample {
        /** The record ID. */
        private Long recordId;
        /** A fixed-length string. */
        private String fixedStr;
        /** A variable-length string. */
        private String varStr;
        /** A float value. */
        private Float floatVal;
        /** A double value. */
        private Double doubleVal;
        /** An 8-bit signed integer value. */
        private Byte int8_Val;
        /** An 8-bit unsigned integer value. */
        private Short uint8_Val;
        /** A 16-bit signed integer value. */
        private Short int16_Val;
        /** A 16-bit unsigned integer value. */
        private Integer uint16_Val;
        /** A 32-bit signed integer value. */
        private Integer int32_Val;
        /** A 32-bit unsigned integer value. */
        private Long uint32_Val;
        /** A 64-bit signed integer value. */
        private Long int64_Val;
        /** A 64-bit unsigned integer value. */
        private BigInteger uint64_Val;
        /** A scaled unsigned integer value as a BigDecimal. */
        private BigDecimal scaledUintVal;
    }

    /**
     * A data class for monitoring data (not used in this example).
     */
    @Data
    public static class MonitoringData {
        /** The site name. */
        private String siteName;
        /** The air quality index. */
        private Float airQualityIndex;
        /** The temperature. */
        private Double temperature;
        /** The sample count. */
        private Integer sampleCount;
    }

    private static final int CYCLE_LENGTH = 5;

    /**
     * Returns a cycled 8-bit signed integer value.
     *
     * @param index the index for cycling
     * @return the cycled value
     */
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

    /**
     * Returns a cycled 16-bit signed integer value.
     *
     * @param index the index for cycling
     * @return the cycled value
     */
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

    /**
     * Returns a cycled 32-bit signed integer value.
     *
     * @param index the index for cycling
     * @return the cycled value
     */
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

    /**
     * Returns a cycled 64-bit signed integer value.
     *
     * @param index the index for cycling
     * @return the cycled value
     */
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

    /**
     * Returns a cycled 8-bit unsigned integer value.
     *
     * @param index the index for cycling
     * @return the cycled value as a short
     */
    public static short getCycledUint8(int index) {
        int cycleIndex = index % CYCLE_LENGTH;
        return switch (cycleIndex) {
            case 0 -> 0;                     // 0x00
            case 1 -> 255 / 4;               // 63 (0x3F) approx
            case 2 -> 255 / 2;               // 127 (0x7F) approx
            case 3 -> (255 / 4) * 3;         // 189 (0xBD) approx
            default -> 255;          // 255 (0xFF)
        };
    }

    /**
     * Returns a cycled 16-bit unsigned integer value.
     *
     * @param index the index for cycling
     * @return the cycled value as an int
     */
    public static int getCycledUint16(int index) {
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

    /**
     * Returns a cycled 32-bit unsigned integer value.
     *
     * @param index the index for cycling
     * @return the cycled value as a long
     */
    public static long getCycledUint32(int index) {
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

    /**
     * Returns a cycled 64-bit unsigned integer value.
     *
     * @param index the index for cycling
     * @return the cycled value as a BigInteger
     */
    public static BigInteger getCycledUint64(int index) {
        int cycleIndex = index % CYCLE_LENGTH;
        BigInteger MAX_U64 = new BigInteger("18446744073709551615"); // 2^64 - 1
        BigInteger FOUR = BigInteger.valueOf(4);
        BigInteger TWO = BigInteger.valueOf(2);
        BigInteger THREE = BigInteger.valueOf(3);

        return switch (cycleIndex) {
            case 0 -> BigInteger.ZERO;
            case 1 -> MAX_U64.divide(FOUR);
            case 2 -> MAX_U64.divide(TWO);
            case 3 -> MAX_U64.divide(FOUR).multiply(THREE);
            default -> MAX_U64;
        };
    }
}