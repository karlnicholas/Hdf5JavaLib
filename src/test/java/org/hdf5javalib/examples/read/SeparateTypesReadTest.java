package org.hdf5javalib.examples.read;

import org.hdf5javalib.dataclass.*;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.datatype.CompoundDatatype;
import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.SeekableByteChannel;
import java.util.BitSet;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class SeparateTypesReadTest {

    public static class Compound {
        private Short a;
        private Double b;

        public Short getA() {
            return a;
        }

        public void setA(Short a) {
            this.a = a;
        }

        public Double getB() {
            return b;
        }

        public void setB(Double b) {
            this.b = b;
        }
    }

    public static class CustomCompound {
        private String name;
        private Short someShort;
        private Double someDouble;

        private CustomCompound() {
        }

        public static Builder builder() {
            return new Builder();
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Short getSomeShort() {
            return someShort;
        }

        public void setSomeShort(Short someShort) {
            this.someShort = someShort;
        }

        public Double getSomeDouble() {
            return someDouble;
        }

        public void setSomeDouble(Double someDouble) {
            this.someDouble = someDouble;
        }

        public static class Builder {
            private final CustomCompound instance = new CustomCompound();

            public Builder name(String name) {
                instance.setName(name);
                return this;
            }

            public Builder someShort(Short someShort) {
                instance.setSomeShort(someShort);
                return this;
            }

            public Builder someDouble(Double someDouble) {
                instance.setSomeDouble(someDouble);
                return this;
            }

            public CustomCompound build() {
                return instance;
            }
        }
    }

    @Test
    void testAllTypesSeparateH5() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("all_types_separate.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();

            // fixed_point
            HdfDataset fixedPointDs = reader.getDataset("/fixed_point").orElseThrow();
            TypedDataSource<HdfFixedPoint> fpSource = new TypedDataSource<>(channel, reader, fixedPointDs, HdfFixedPoint.class);
            HdfFixedPoint fp = fpSource.readScalar();
            assertEquals(42, fp.getInstance(Integer.class).intValue());
            TypedDataSource<Integer> intSource = new TypedDataSource<>(channel, reader, fixedPointDs, Integer.class);
            assertEquals(42, intSource.readScalar().intValue());
            TypedDataSource<Long> longSource = new TypedDataSource<>(channel, reader, fixedPointDs, Long.class);
            assertEquals(42L, longSource.readScalar().longValue());
            TypedDataSource<BigInteger> biSource = new TypedDataSource<>(channel, reader, fixedPointDs, BigInteger.class);
            assertEquals(BigInteger.valueOf(42), biSource.readScalar());
            TypedDataSource<BigDecimal> bdSource = new TypedDataSource<>(channel, reader, fixedPointDs, BigDecimal.class);
            assertEquals(BigDecimal.valueOf(42), bdSource.readScalar());
            TypedDataSource<String> strSource = new TypedDataSource<>(channel, reader, fixedPointDs, String.class);
            assertEquals("42", strSource.readScalar());

            // float
            HdfDataset floatDs = reader.getDataset("/float").orElseThrow();
            TypedDataSource<HdfFloatPoint> floatPointSource = new TypedDataSource<>(channel, reader, floatDs, HdfFloatPoint.class);
            HdfFloatPoint floatPoint = floatPointSource.readScalar();
            assertEquals(3.14f, floatPoint.getInstance(Float.class), 0.001f);
            TypedDataSource<Float> floatJavaSource = new TypedDataSource<>(channel, reader, floatDs, Float.class);
            assertEquals(3.14f, floatJavaSource.readScalar(), 0.001f);
            TypedDataSource<Double> doubleSource = new TypedDataSource<>(channel, reader, floatDs, Double.class);
            assertEquals(3.140000104904175, doubleSource.readScalar(), 0.000001);
            TypedDataSource<String> floatStrSource = new TypedDataSource<>(channel, reader, floatDs, String.class);
            assertEquals("3.14", floatStrSource.readScalar());

            // time
            HdfDataset timeDs = reader.getDataset("/time").orElseThrow();
            TypedDataSource<HdfTime> timeSource = new TypedDataSource<>(channel, reader, timeDs, HdfTime.class);
            HdfTime time = timeSource.readScalar();
            assertEquals(1672531200L, time.getInstance(Long.class).longValue());
            TypedDataSource<Long> timeLongSource = new TypedDataSource<>(channel, reader, timeDs, Long.class);
            assertEquals(1672531200L, timeLongSource.readScalar().longValue());
            TypedDataSource<BigInteger> timeBiSource = new TypedDataSource<>(channel, reader, timeDs, BigInteger.class);
            assertEquals(BigInteger.valueOf(1672531200L), timeBiSource.readScalar());
            TypedDataSource<String> timeStrSource = new TypedDataSource<>(channel, reader, timeDs, String.class);
            assertEquals("1672531200", timeStrSource.readScalar());

            // string
            HdfDataset stringDs = reader.getDataset("/string").orElseThrow();
            TypedDataSource<HdfString> hdfStringSource = new TypedDataSource<>(channel, reader, stringDs, HdfString.class);
            HdfString hdfString = hdfStringSource.readScalar();
            assertEquals("Hello HDF5!", hdfString.getInstance(String.class));
            TypedDataSource<String> javaStringSource = new TypedDataSource<>(channel, reader, stringDs, String.class);
            assertEquals("Hello HDF5!", javaStringSource.readScalar());

            // bitfield
            HdfDataset bitfieldDs = reader.getDataset("/bitfield").orElseThrow();
            TypedDataSource<HdfBitField> bitFieldSource = new TypedDataSource<>(channel, reader, bitfieldDs, HdfBitField.class);
            HdfBitField bitField = bitFieldSource.readScalar();
            assertEquals("10101010", bitField.getInstance(String.class));
            TypedDataSource<BitSet> bitSetSource = new TypedDataSource<>(channel, reader, bitfieldDs, BitSet.class);
            BitSet expectedBitSet = new BitSet();
            expectedBitSet.set(1);
            expectedBitSet.set(3);
            expectedBitSet.set(5);
            expectedBitSet.set(7);
            assertEquals(expectedBitSet, bitSetSource.readScalar());
            TypedDataSource<String> bitStrSource = new TypedDataSource<>(channel, reader, bitfieldDs, String.class);
            assertEquals("10101010", bitStrSource.readScalar());

            // compound
            HdfDataset compoundDs = reader.getDataset("/compound").orElseThrow();
            TypedDataSource<HdfCompound> hdfCompoundSource = new TypedDataSource<>(channel, reader, compoundDs, HdfCompound.class);
            HdfCompound hdfCompound = hdfCompoundSource.readScalar();
            assertEquals("123, 9.81", hdfCompound.getInstance(String.class));
            TypedDataSource<String> compoundStrSource = new TypedDataSource<>(channel, reader, compoundDs, String.class);
            assertEquals("123, 9.81", compoundStrSource.readScalar());
            TypedDataSource<Compound> compoundJavaSource = new TypedDataSource<>(channel, reader, compoundDs, Compound.class);
            Compound compoundJava = compoundJavaSource.readScalar();
            assertEquals(Short.valueOf((short)123), compoundJava.getA());
            assertEquals(9.81, compoundJava.getB(), 0.001);
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
            TypedDataSource<CustomCompound> customCompoundSource = new TypedDataSource<>(channel, reader, compoundDs, CustomCompound.class);
            CustomCompound customCompound = customCompoundSource.readScalar();
            assertEquals("Name", customCompound.getName());
            assertEquals(Short.valueOf((short)123), customCompound.getSomeShort());
            assertEquals(9.81, customCompound.getSomeDouble(), 0.001);

            // opaque
            HdfDataset opaqueDs = reader.getDataset("/opaque").orElseThrow();
            TypedDataSource<HdfOpaque> opaqueSource = new TypedDataSource<>(channel, reader, opaqueDs, HdfOpaque.class);
            HdfOpaque opaque = opaqueSource.readScalar();
            assertEquals("DEADBEEF", opaque.getInstance(String.class));
            TypedDataSource<String> opaqueStrSource = new TypedDataSource<>(channel, reader, opaqueDs, String.class);
            assertEquals("DEADBEEF", opaqueStrSource.readScalar());
            TypedDataSource<byte[]> opaqueByteSource = new TypedDataSource<>(channel, reader, opaqueDs, byte[].class);
            byte[] expectedOpaqueBytes = {-34, -83, -66, -17};
            assertArrayEquals(expectedOpaqueBytes, opaqueByteSource.readScalar());

            // reference
            HdfDataset referenceDs = reader.getDataset("/reference").orElseThrow();
            TypedDataSource<HdfReference> referenceSource = new TypedDataSource<>(channel, reader, referenceDs, HdfReference.class);
            HdfReference reference = referenceSource.readScalar();
            assertEquals("/target", reference.getInstance(String.class));
            TypedDataSource<String> referenceStrSource = new TypedDataSource<>(channel, reader, referenceDs, String.class);
            assertEquals("/target", referenceStrSource.readScalar());
            TypedDataSource<byte[]> referenceByteSource = new TypedDataSource<>(channel, reader, referenceDs, byte[].class);
            byte[] expectedReferenceBytes = {64, 20, 0, 0, 0, 0, 0, 0};
            assertArrayEquals(expectedReferenceBytes, referenceByteSource.readScalar());

            // enum
            HdfDataset enumDs = reader.getDataset("/enum").orElseThrow();
            TypedDataSource<HdfEnum> enumSource = new TypedDataSource<>(channel, reader, enumDs, HdfEnum.class);
            HdfEnum hdfEnum = enumSource.readScalar();
            assertEquals("GREEN", hdfEnum.getInstance(String.class));
            TypedDataSource<String> enumStrSource = new TypedDataSource<>(channel, reader, enumDs, String.class);
            assertEquals("GREEN", enumStrSource.readScalar());

            // vlen
            HdfDataset vlenDs = reader.getDataset("/vlen").orElseThrow();
            TypedDataSource<HdfVariableLength> vlenSource = new TypedDataSource<>(channel, reader, vlenDs, HdfVariableLength.class);
            HdfVariableLength vlen = vlenSource.readScalar();
            assertEquals("[7, 8, 9]", vlen.getInstance(String.class));
            TypedDataSource<String> vlenStrSource = new TypedDataSource<>(channel, reader, vlenDs, String.class);
            assertEquals("[7, 8, 9]", vlenStrSource.readScalar());
            TypedDataSource<Object> vlenObjSource = new TypedDataSource<>(channel, reader, vlenDs, Object.class);
            HdfData[] vlenData = (HdfData[]) vlenObjSource.readScalar();
            assertEquals(7, vlenData[0].getInstance(Integer.class).intValue());
            assertEquals(8, vlenData[1].getInstance(Integer.class).intValue());
            assertEquals(9, vlenData[2].getInstance(Integer.class).intValue());

            // array
            HdfDataset arrayDs = reader.getDataset("/array").orElseThrow();
            TypedDataSource<HdfArray> arraySource = new TypedDataSource<>(channel, reader, arrayDs, HdfArray.class);
            HdfArray hdfArray = arraySource.readScalar();
            assertEquals("[10, 20, 30]", hdfArray.getInstance(String.class));
            TypedDataSource<String> arrayStrSource = new TypedDataSource<>(channel, reader, arrayDs, String.class);
            assertEquals("[10, 20, 30]", arrayStrSource.readScalar());
            TypedDataSource<HdfData[]> arrayDataSource = new TypedDataSource<>(channel, reader, arrayDs, HdfData[].class);
            HdfData[] arrayData = arrayDataSource.readScalar();
            assertEquals(10, arrayData[0].getInstance(Integer.class).intValue());
            assertEquals(20, arrayData[1].getInstance(Integer.class).intValue());
            assertEquals(30, arrayData[2].getInstance(Integer.class).intValue());

            assertEquals("HdfSuperblock{version=0, freeSpaceVersion=0, rootGroupVersion=0, sharedHeaderVersion=0, sizeOfOffsets=8, sizeOfLengths=8, groupLeafNodeK=4, groupInternalNodeK=16, baseAddress=0, freeSpaceAddress=<Undefined>, endOfFileAddress=11144, driverInformationAddress=<Undefined>}", reader.getSuperblock().toString());
        }
    }
}