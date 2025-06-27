package org.hdf5javalib.maydo.examples.read;

import org.hdf5javalib.maydo.HdfFileReader;
import org.hdf5javalib.maydo.dataclass.*;
import org.hdf5javalib.maydo.datatype.CompoundDatatype;
import org.hdf5javalib.maydo.hdfjava.HdfDataset;
import org.hdf5javalib.maydo.utils.HdfDisplayUtils;

import java.io.FileInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Demonstrates reading various HDF5 data types from separate datasets.
 * <p>
 * The {@code HdfSeparateTypesRead} class is an example application that reads
 * datasets containing different HDF5 data types (e.g., fixed-point, floating-point,
 * string, compound, etc.) from an HDF5 file. It uses {@link HdfDisplayUtils} to
 * display the data in multiple formats, including native HDF5 types and Java types,
 * and demonstrates custom type conversion for compound datasets.
 * </p>
 */
public class SeparateTypesRead {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SeparateTypesRead.class);
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) throws Exception {
        new SeparateTypesRead().run();
    }

    /**
     * Executes the main logic of reading and displaying various data types from an HDF5 file.
     */
    private void run() throws Exception {
        String filePath = Objects.requireNonNull(this.getClass().getResource("/all_types_separate.h5")).getFile();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            FileChannel channel = fis.getChannel();
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("/fixed_point").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfFixedPoint.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, Integer.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, Long.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, BigInteger.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, BigDecimal.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("float").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfFloatPoint.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, Float.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, Double.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("time").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfTime.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, Long.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, BigInteger.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("string").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfString.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("bitfield").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfBitField.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, BitSet.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("compound").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfCompound.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, Compound.class, reader);
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
                HdfDisplayUtils.displayScalarData(channel, dataSet, CustomCompound.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("opaque").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfOpaque.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, byte[].class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("reference").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfReference.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, byte[].class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("enum").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfEnum.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("vlen").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfVariableLength.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, Object.class, reader);
            }
            try (HdfDataset dataSet = reader.getRootGroup().getDataset("array").orElseThrow()) {
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfArray.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                HdfDisplayUtils.displayScalarData(channel, dataSet, HdfData[].class, reader);
            }
            log.info("Superblock: {}", reader.getSuperblock());
        }
    }

    /**
     * A data class representing a basic compound dataset with two fields.
     */
    public static class Compound {
        /** A short integer field. */
        private Short a;
        /** A double-precision floating-point field. */
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

    /**
     * A custom data class for a compound dataset with renamed fields.
     */
    public static class CustomCompound {
        /** A name identifier for the compound. */
        private String name;
        /** A short integer field (mapped from field 'a'). */
        private Short someShort;
        /** A double-precision floating-point field (mapped from field 'b'). */
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
}