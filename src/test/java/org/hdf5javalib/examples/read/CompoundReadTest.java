package org.hdf5javalib.examples.read;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CompoundReadTest {
    private static final String FULL_RECORD_STRING = "1000, FixedData, varStr:1, 5.877472E-39, 1.1125369292536007E-308, -128, 0, -32768, 0, -2147483648, 0, -9223372036854775808, 0, 1.0000000";
    private static final List<BigDecimal> TEN_BIG_DECIMALS = List.of(
            new BigDecimal("1.0000000"), new BigDecimal("2.2500000"), new BigDecimal("3.5000000"),
            new BigDecimal("4.7500000"), new BigDecimal("5.0000000"), new BigDecimal("6.2500000"),
            new BigDecimal("7.5000000"), new BigDecimal("8.7500000"), new BigDecimal("9.0000000"),
            new BigDecimal("10.2500000"));

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

        public Long getRecordId() {
            return recordId;
        }

        public void setRecordId(Long recordId) {
            this.recordId = recordId;
        }

        public String getFixedStr() {
            return fixedStr;
        }

        public void setFixedStr(String fixedStr) {
            this.fixedStr = fixedStr;
        }

        public String getVarStr() {
            return varStr;
        }

        public void setVarStr(String varStr) {
            this.varStr = varStr;
        }

        public Float getFloatVal() {
            return floatVal;
        }

        public void setFloatVal(Float floatVal) {
            this.floatVal = floatVal;
        }

        public Double getDoubleVal() {
            return doubleVal;
        }

        public void setDoubleVal(Double doubleVal) {
            this.doubleVal = doubleVal;
        }

        public Byte getInt8_Val() {
            return int8_Val;
        }

        public void setInt8_Val(Byte int8_Val) {
            this.int8_Val = int8_Val;
        }

        public Short getInt16_Val() {
            return int16_Val;
        }

        public void setInt16_Val(Short int16_Val) {
            this.int16_Val = int16_Val;
        }

        public Integer getInt32_Val() {
            return int32_Val;
        }

        public void setInt32_Val(Integer int32_Val) {
            this.int32_Val = int32_Val;
        }

        public Long getInt64_Val() {
            return int64_Val;
        }

        public void setInt64_Val(Long int64_Val) {
            this.int64_Val = int64_Val;
        }

        public Short getUint8_Val() {
            return uint8_Val;
        }

        public void setUint8_Val(Short uint8_Val) {
            this.uint8_Val = uint8_Val;
        }

        public Integer getUint16_Val() {
            return uint16_Val;
        }

        public void setUint16_Val(Integer uint16_Val) {
            this.uint16_Val = uint16_Val;
        }

        public Long getUint32_Val() {
            return uint32_Val;
        }

        public void setUint32_Val(Long uint32_Val) {
            this.uint32_Val = uint32_Val;
        }

        public BigInteger getUint64_Val() {
            return uint64_Val;
        }

        public void setUint64_Val(BigInteger uint64_Val) {
            this.uint64_Val = uint64_Val;
        }

        public BigDecimal getScaledUintVal() {
            return scaledUintVal;
        }

        public void setScaledUintVal(BigDecimal scaledUintVal) {
            this.scaledUintVal = scaledUintVal;
        }
    }

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
            datatype.getMembers().forEach(member -> {
                byte[] memberBytes = Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset() + member.getSize());
                switch (member.getName()) {
                    case "recordId" -> {
                        long recordId = member.getInstance(Long.class, memberBytes);
                        monitoringData.setSampleCount((int) (recordId - 1000));
                    }
                    case "fixedStr" -> monitoringData.setSiteName(member.getInstance(String.class, memberBytes));
                    case "floatVal" -> monitoringData.setAirQualityIndex(member.getInstance(Float.class, memberBytes));
                    case "doubleVal" -> monitoringData.setTemperature(member.getInstance(Double.class, memberBytes));
                    default -> {} // Ignore other fields
                }
            });
            return monitoringData;
        });
    }

    @Test
    void testCompoundExampleH5() throws IOException {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("compound_example.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.getRootGroup().findDataset("CompoundData");

            // Test with CompoundExample
            TypedDataSource<CompoundExample> compoundSource = new TypedDataSource<>(channel, reader, dataSet, CompoundExample.class);
            CompoundExample[] compoundRecords = compoundSource.readVector();
            assertEquals(1000, compoundRecords.length);

            CompoundExample first = compoundRecords[0];
            assertEquals(1000L, first.getRecordId());
            assertEquals("FixedData", first.getFixedStr());
            assertEquals("varStr:1", first.getVarStr());
            assertEquals(5.8774717541114375E-39F, first.getFloatVal(), 1E-45F);
            assertEquals(1.1125369292536007E-308, first.getDoubleVal(), 1E-310);
            assertEquals(-128, first.getInt8_Val().byteValue());
            assertEquals(0, first.getUint8_Val().shortValue());
            assertEquals(-32768, first.getInt16_Val().shortValue());
            assertEquals(0, first.getUint16_Val().intValue());
            assertEquals(-2147483648, first.getInt32_Val().intValue());
            assertEquals(0L, first.getUint32_Val().longValue());
            assertEquals(-9223372036854775808L, first.getInt64_Val().longValue());
            assertEquals(BigInteger.ZERO, first.getUint64_Val());
            assertEquals(new BigDecimal("1.0000000"), first.getScaledUintVal());

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
                    .filter(c -> c.getMembers().get(0).getInstance(Long.class) < 1010)
                    .map(c -> c.getMembers().get(13).getInstance(BigDecimal.class))
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
    void testCustomConverter() throws IOException {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("compound_example.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataSet dataSet = reader.getRootGroup().findDataset("CompoundData");

            TypedDataSource<MonitoringData> dataSource = new TypedDataSource<>(channel, reader, dataSet, MonitoringData.class);
            MonitoringData[] allData = dataSource.readVector();
            assertEquals(1000, allData.length);

            // Verify first record
            MonitoringData first = allData[0];
            assertEquals("FixedData", first.getSiteName());
            assertEquals(5.8774717541114375E-39F, first.getAirQualityIndex(), 1E-45F);
            assertEquals(1.1125369292536007E-308, first.getTemperature(), 1E-310);
            assertEquals(0, first.getSampleCount().intValue()); // 1000 - 1000

            // Verify streaming
            List<MonitoringData> streamedData = dataSource.streamVector().toList();
            assertEquals(1000, streamedData.size());
            MonitoringData firstStreamed = streamedData.get(0);
            assertEquals("FixedData", firstStreamed.getSiteName());
            assertEquals(5.8774717541114375E-39F, firstStreamed.getAirQualityIndex(), 1E-45F);
            assertEquals(1.1125369292536007E-308, firstStreamed.getTemperature(), 1E-310);
            assertEquals(0, firstStreamed.getSampleCount().intValue());
        }
    }
}