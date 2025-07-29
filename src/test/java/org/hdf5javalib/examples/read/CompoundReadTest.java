package org.hdf5javalib.examples.read;

import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.datatype.CompoundDatatype;
import org.hdf5javalib.datatype.CompoundMemberDatatype;
import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CompoundReadTest {
    private static final String FULL_RECORD_STRING = "1000, FixedData, varStr:1, 0.0, 0.0, -128, 0, -32768, 0, -2147483648, 0, -9223372036854775808, 0, 1.0000000";
    private static final List<BigDecimal> TEN_BIG_DECIMALS = List.of(
            new BigDecimal("1.0000000"), new BigDecimal("2.2500000"), new BigDecimal("3.5000000"),
            new BigDecimal("4.7500000"), new BigDecimal("5.0000000"), new BigDecimal("6.2500000"),
            new BigDecimal("7.5000000"), new BigDecimal("8.7500000"), new BigDecimal("9.0000000"),
            new BigDecimal("10.2500000"));

    public record CompoundExample(
            Long recordId,
            String fixedStr,
            String varStr,
            Float floatVal,
            Double doubleVal,
            Byte int8_Val,
            Short int16_Val,
            Integer int32_Val,
            Long int64_Val,
            Short uint8_Val,
            Integer uint16_Val,
            Long uint32_Val,
            BigInteger uint64_Val,
            BigDecimal scaledUintVal
    ) {}

    public static class MonitoringData {
        private String siteName;
        private Float airQualityIndex;
        private Double temperature;
        private Integer sampleCount;

        public String getSiteName() {
            return siteName;
        }

        public void setSiteName(String siteName) {
            this.siteName = siteName;
        }

        public Float getAirQualityIndex() {
            return airQualityIndex;
        }

        public void setAirQualityIndex(Float airQualityIndex) {
            this.airQualityIndex = airQualityIndex;
        }

        public Double getTemperature() {
            return temperature;
        }

        public void setTemperature(Double temperature) {
            this.temperature = temperature;
        }

        public Integer getSampleCount() {
            return sampleCount;
        }

        public void setSampleCount(Integer sampleCount) {
            this.sampleCount = sampleCount;
        }
    }

    @BeforeAll
    static void registerCustomConverter() {
        CompoundDatatype.addConverter(MonitoringData.class, (bytes, datatype) -> {
            MonitoringData monitoringData = new MonitoringData();
            for (CompoundMemberDatatype member : datatype.getMembers()) {
                byte[] memberBytes = Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset() + member.getSize());
                switch (member.getName()) {
                    case "recordId" -> {
                        long recordId = member.getInstance(Long.class, memberBytes);
                        monitoringData.setSampleCount((int) (recordId - 1000));
                    }
                    case "fixedStr" -> monitoringData.setSiteName(member.getInstance(String.class, memberBytes));
                    case "floatVal" -> monitoringData.setAirQualityIndex(member.getInstance(Float.class, memberBytes));
                    case "doubleVal" -> monitoringData.setTemperature(member.getInstance(Double.class, memberBytes));
                    default -> {
                    } // Ignore other fields
                }
            }
            return monitoringData;
        });
    }

    @Test
    void testCompoundExampleH5() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("compound_example.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("CompoundData").orElseThrow();

            // Test with CompoundExample
            TypedDataSource<CompoundExample> compoundSource = new TypedDataSource<>(channel, reader, dataSet, CompoundExample.class);
            CompoundExample[] compoundRecords = compoundSource.readVector();
            assertEquals(1000, compoundRecords.length);

            CompoundExample first = compoundRecords[0];
            assertEquals(1000L, first.recordId());
            assertEquals("FixedData", first.fixedStr());
            assertEquals("varStr:1", first.varStr());
            assertEquals(0.0, first.floatVal(), 0.0);
            assertEquals(0.0, first.doubleVal(), 0.0);
            assertEquals(-128, first.int8_Val().byteValue());
            assertEquals(0, first.uint8_Val().shortValue());
            assertEquals(-32768, first.int16_Val().shortValue());
            assertEquals(0, first.uint16_Val().intValue());
            assertEquals(-2147483648, first.int32_Val().intValue());
            assertEquals(0L, first.uint32_Val().longValue());
            assertEquals(-9223372036854775808L, first.int64_Val().longValue());
            assertEquals(BigInteger.ZERO, first.uint64_Val());
            assertEquals(new BigDecimal("1.0000000"), first.scaledUintVal());

            // Test with HdfCompound
            TypedDataSource<HdfCompound> hdfCompoundSource = new TypedDataSource<>(channel, reader, dataSet, HdfCompound.class);
            HdfCompound[] hdfRecords = hdfCompoundSource.readVector();
            assertEquals(1000, hdfRecords.length);
            HdfCompound hdfFirst = hdfRecords[0];
            assertEquals(14, hdfFirst.getMembers().size());
            assertEquals(1000L, hdfFirst.getMembers().get(0).getInstance(Long.class));
            assertEquals("FixedData", hdfFirst.getMembers().get(1).getInstance(String.class));
            assertEquals("varStr:1", hdfFirst.getMembers().get(2).getInstance(String.class));
            assertEquals(new BigDecimal("1.0000000"), hdfFirst.getMembers().get(13).getInstance(BigDecimal.class));

            // Test with String
            TypedDataSource<String> stringSource = new TypedDataSource<>(channel, reader, dataSet, String.class);
            String[] stringRecords = stringSource.readVector();
            assertEquals(1000, stringRecords.length);
            assertEquals(FULL_RECORD_STRING, stringRecords[0]);

            // Test BigDecimal extraction from HdfCompound
            List<BigDecimal> firstTenBigDecimals = hdfCompoundSource.streamVector()
                    .filter(c -> {
                        try {
                            return c.getMembers().get(0).getInstance(Long.class) < 1010;
                        } catch (IOException | InvocationTargetException | InstantiationException |
                                 IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .map(c -> {
                        try {
                            return c.getMembers().get(13).getInstance(BigDecimal.class);
                        } catch (IOException | InvocationTargetException | InstantiationException |
                                 IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toList();
            assertEquals(10, firstTenBigDecimals.size());
            assertEquals(TEN_BIG_DECIMALS, firstTenBigDecimals);

            // Test with HdfData (same as HdfCompound)
            TypedDataSource<HdfData> hdfDataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] hdfDataRecords = hdfDataSource.readVector();
            assertEquals(1000, hdfDataRecords.length);
            HdfData hdfDataFirst = hdfDataRecords[0];
            assertEquals(14, ((HdfCompound) hdfDataFirst).getMembers().size());
            assertEquals(1000L, ((HdfCompound) hdfDataFirst).getMembers().get(0).getInstance(Long.class));

            // Test with byte[][]
            TypedDataSource<byte[][]> byteArraySource = new TypedDataSource<>(channel, reader, dataSet, byte[][].class);
            byte[][][] byteArrayRecords = byteArraySource.readVector();
            assertEquals(1000, byteArrayRecords.length);
            byte[][] firstByteArray = byteArrayRecords[0];
            assertEquals(14, firstByteArray.length); // 14 members
            assertEquals(1000L, ByteBuffer.wrap(firstByteArray[0]).order(ByteOrder.LITTLE_ENDIAN).getLong());

            // Test with HdfData[]
            TypedDataSource<HdfData[]> hdfDataArraySource = new TypedDataSource<>(channel, reader, dataSet, HdfData[].class);
            HdfData[][] hdfDataArrayRecords = hdfDataArraySource.readVector();
            assertEquals(1000, hdfDataArrayRecords.length);
            HdfData[] firstHdfDataArray = hdfDataArrayRecords[0];
            assertEquals(14, firstHdfDataArray.length);
            assertEquals(1000L, firstHdfDataArray[0].getInstance(Long.class));
        }
    }

    @Test
    void testCustomConverter() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("compound_example.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("CompoundData").orElseThrow();

            TypedDataSource<MonitoringData> dataSource = new TypedDataSource<>(channel, reader, dataSet, MonitoringData.class);
            MonitoringData[] allData = dataSource.readVector();
            assertEquals(1000, allData.length);

            // Verify first record
            MonitoringData first = allData[0];
            assertEquals("FixedData", first.getSiteName());
            assertEquals(0.0, first.getAirQualityIndex(), 0.0);
            assertEquals(0.0, first.getTemperature(), 0.0);
            assertEquals(0, first.getSampleCount().intValue()); // 1000 - 1000

            // Verify streaming
            List<MonitoringData> streamedData = dataSource.streamVector().toList();
            assertEquals(1000, streamedData.size());
            MonitoringData firstStreamed = streamedData.get(0);
            assertEquals("FixedData", firstStreamed.getSiteName());
            assertEquals(0.0, firstStreamed.getAirQualityIndex(), 0.0);
            assertEquals(0.0, firstStreamed.getTemperature(), 0.0);
            assertEquals(0, firstStreamed.getSampleCount().intValue());
        }
    }
}