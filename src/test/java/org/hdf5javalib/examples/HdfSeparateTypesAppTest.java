package org.hdf5javalib.examples;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.*;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HdfSeparateTypesAppTest {
    private static Path getResourcePath() {
        String resourcePath = Objects.requireNonNull(HdfSeparateTypesAppTest.class.getClassLoader().getResource("all_types_separate.h5")).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @Data
    public static class Compound {
        private Short a;
        private Double b;
    }

    @Data
    @Builder
    public static class CustomCompound {
        private String name;
        private Short someShort;
        private Double someDouble;
    }

    @BeforeAll
    static void registerCustomConverter() {
        CompoundDatatype.addConverter(CustomCompound.class, (bytes, compoundDataType) -> {
            Map<String, HdfCompoundMember> nameToMember = compoundDataType.getInstance(HdfCompound.class, bytes)
                    .getMembers()
                    .stream()
                    .collect(Collectors.toMap(m -> m.getDatatype().getName(), m -> m));
            return CustomCompound.builder()
                    .name("Name")
                    .someShort(nameToMember.get("a").getInstance(Short.class))
                    .someDouble(nameToMember.get("b").getInstance(Double.class))
                    .build();
        });
    }

    @Test
    void testAllTypesSeparateH5() throws IOException {
        Path filePath = getResourcePath();
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();

            // Fixed Point (32-bit signed, 42)
            HdfDataSet fixedPointDataSet = reader.findDataset("fixed_point", channel, reader.getRootGroup());
            TypedDataSource<HdfFixedPoint> fpSource = new TypedDataSource<>(channel, reader, fixedPointDataSet, HdfFixedPoint.class);
            assertEquals(42, fpSource.readScalar().getInstance(Integer.class));
            assertEquals(42, new TypedDataSource<>(channel, reader, fixedPointDataSet, Integer.class).readScalar());
            assertEquals(42L, new TypedDataSource<>(channel, reader, fixedPointDataSet, Long.class).readScalar());
            assertEquals(BigInteger.valueOf(42), new TypedDataSource<>(channel, reader, fixedPointDataSet, BigInteger.class).readScalar());
            assertEquals(BigDecimal.valueOf(42), new TypedDataSource<>(channel, reader, fixedPointDataSet, BigDecimal.class).readScalar());
            assertEquals("42", new TypedDataSource<>(channel, reader, fixedPointDataSet, String.class).readScalar());

            // Float (32-bit float, 3.14)
            HdfDataSet floatDataSet = reader.findDataset("float", channel, reader.getRootGroup());
            TypedDataSource<HdfFloatPoint> floatSource = new TypedDataSource<>(channel, reader, floatDataSet, HdfFloatPoint.class);
            assertEquals(3.14f, floatSource.readScalar().getInstance(Float.class), 0.001f);
            assertEquals(3.14f, new TypedDataSource<>(channel, reader, floatDataSet, Float.class).readScalar(), 0.001f);
            assertEquals(3.14, new TypedDataSource<>(channel, reader, floatDataSet, Double.class).readScalar(), 0.001);
            assertEquals("3.140000104904175", new TypedDataSource<>(channel, reader, floatDataSet, String.class).readScalar());

            // Time (64-bit integer, 1672531200, no real-time association)
            HdfDataSet timeDataSet = reader.findDataset("time", channel, reader.getRootGroup());
            TypedDataSource<HdfTime> timeSource = new TypedDataSource<>(channel, reader, timeDataSet, HdfTime.class);
            assertEquals(1672531200L, timeSource.readScalar().getInstance(Long.class));
            assertEquals(1672531200L, new TypedDataSource<>(channel, reader, timeDataSet, Long.class).readScalar());
            assertEquals(BigInteger.valueOf(1672531200L), new TypedDataSource<>(channel, reader, timeDataSet, BigInteger.class).readScalar());
            assertEquals("1672531200", new TypedDataSource<>(channel, reader, timeDataSet, String.class).readScalar());

            // String (16-byte ASCII, "Hello HDF5!")
            HdfDataSet stringDataSet = reader.findDataset("string", channel, reader.getRootGroup());
            TypedDataSource<HdfString> stringSource = new TypedDataSource<>(channel, reader, stringDataSet, HdfString.class);
            assertEquals("Hello HDF5!", stringSource.readScalar().getInstance(String.class));
            assertEquals("Hello HDF5!", new TypedDataSource<>(channel, reader, stringDataSet, String.class).readScalar());

            // Bitfield (8-bit, 0b10101010 = {1,3,5,7})
            HdfDataSet bitfieldDataSet = reader.findDataset("bitfield", channel, reader.getRootGroup());
            TypedDataSource<HdfBitField> bitfieldSource = new TypedDataSource<>(channel, reader, bitfieldDataSet, HdfBitField.class);
            BitSet bitSet = bitfieldSource.readScalar().getInstance(BitSet.class);
            assertEquals(BitSet.valueOf(new byte[]{(byte) 0b10101010}), bitSet);
            assertEquals(BitSet.valueOf(new byte[]{(byte) 0b10101010}), new TypedDataSource<>(channel, reader, bitfieldDataSet, BitSet.class).readScalar());
            assertEquals("10101010", new TypedDataSource<>(channel, reader, bitfieldDataSet, String.class).readScalar());

            // Compound (a=123 short, b=9.81 double)
            HdfDataSet compoundDataSet = reader.findDataset("compound", channel, reader.getRootGroup());
            TypedDataSource<HdfCompound> compoundSource = new TypedDataSource<>(channel, reader, compoundDataSet, HdfCompound.class);
            HdfCompound compound = compoundSource.readScalar();
            assertEquals(123, compound.getMembers().get(0).getInstance(Short.class).intValue());
            assertEquals(9.81, compound.getMembers().get(1).getInstance(Double.class), 0.001);
            assertEquals("123, 9.81", new TypedDataSource<>(channel, reader, compoundDataSet, String.class).readScalar());
            Compound basicCompound = new TypedDataSource<>(channel, reader, compoundDataSet, Compound.class).readScalar();
            assertEquals(123, basicCompound.getA().intValue());
            assertEquals(9.81, basicCompound.getB(), 0.001);
            CustomCompound customCompound = new TypedDataSource<>(channel, reader, compoundDataSet, CustomCompound.class).readScalar();
            assertEquals("Name", customCompound.getName());
            assertEquals(123, customCompound.getSomeShort().intValue());
            assertEquals(9.81, customCompound.getSomeDouble(), 0.001);

            // Opaque (4-byte, DEADBEEF)
            HdfDataSet opaqueDataSet = reader.findDataset("opaque", channel, reader.getRootGroup());
            TypedDataSource<HdfOpaque> opaqueSource = new TypedDataSource<>(channel, reader, opaqueDataSet, HdfOpaque.class);
            assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, opaqueSource.readScalar().getInstance(byte[].class));
            assertEquals("DEADBEEF", new TypedDataSource<>(channel, reader, opaqueDataSet, String.class).readScalar());
            assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, new TypedDataSource<>(channel, reader, opaqueDataSet, byte[].class).readScalar());

            // Reference (8-byte object reference)
            HdfDataSet referenceDataSet = reader.findDataset("reference", channel, reader.getRootGroup());
            TypedDataSource<HdfReference> refSource = new TypedDataSource<>(channel, reader, referenceDataSet, HdfReference.class);
            assertArrayEquals(new byte[]{0x40, 0x14, 0, 0, 0, 0, 0, 0}, refSource.readScalar().getInstance(byte[].class));
            assertEquals("Reference[Object Reference]=4014000000000000", new TypedDataSource<>(channel, reader, referenceDataSet, String.class).readScalar());
            assertArrayEquals(new byte[]{0x40, 0x14, 0, 0, 0, 0, 0, 0}, new TypedDataSource<>(channel, reader, referenceDataSet, byte[].class).readScalar());

            // Enum (1-byte, "GREEN")
            HdfDataSet enumDataSet = reader.findDataset("enum", channel, reader.getRootGroup());
            TypedDataSource<HdfEnum> enumSource = new TypedDataSource<>(channel, reader, enumDataSet, HdfEnum.class);
            assertEquals("GREEN", enumSource.readScalar().getInstance(String.class));
            assertEquals("GREEN", new TypedDataSource<>(channel, reader, enumDataSet, String.class).readScalar());

            // Variable Length (sequence of ints, [7, 8, 9])
            HdfDataSet vlenDataSet = reader.findDataset("vlen", channel, reader.getRootGroup());
            TypedDataSource<HdfVariableLength> vlenSource = new TypedDataSource<>(channel, reader, vlenDataSet, HdfVariableLength.class);
            assertEquals("[7, 8, 9]", vlenSource.readScalar().getInstance(String.class));
            assertEquals("[7, 8, 9]", new TypedDataSource<>(channel, reader, vlenDataSet, String.class).readScalar());
            HdfData[] vlenArray = new TypedDataSource<>(channel, reader, vlenDataSet, HdfData[].class).readScalar();
            assertEquals(3, vlenArray.length);
            assertEquals(7, vlenArray[0].getInstance(Integer.class));
            assertEquals(8, vlenArray[1].getInstance(Integer.class));
            assertEquals(9, vlenArray[2].getInstance(Integer.class));

            // Array (3x 32-bit ints, [10, 20, 30])
            HdfDataSet arrayDataSet = reader.findDataset("array", channel, reader.getRootGroup());
            TypedDataSource<HdfArray> arraySource = new TypedDataSource<>(channel, reader, arrayDataSet, HdfArray.class);
            assertEquals("[10, 20, 30]", arraySource.readScalar().getInstance(String.class));
            assertEquals("[10, 20, 30]", new TypedDataSource<>(channel, reader, arrayDataSet, String.class).readScalar());
            HdfData[] arrayData = new TypedDataSource<>(channel, reader, arrayDataSet, HdfData[].class).readScalar();
            assertEquals(3, arrayData.length);
            assertEquals(10, arrayData[0].getInstance(Integer.class));
            assertEquals(20, arrayData[1].getInstance(Integer.class));
            assertEquals(30, arrayData[2].getInstance(Integer.class));
        }
    }
}