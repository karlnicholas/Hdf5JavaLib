package org.hdf5javalib.examples;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.*;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;
import org.hdf5javalib.utils.HdfTestUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class HdfSeparateTypesApp {
    public static void main(String[] args) {
        new HdfSeparateTypesApp().run();
    }

    private void run() {
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = Objects.requireNonNull(this.getClass().getResource("/all_types_separate.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
//                try ( HdfDataSet dataSet = reader.findDataset("fixed_point", channel, reader.getRootGroup()) ) {
//                    HdfTestUtils.displayScalarData(channel, dataSet, HdfFixedPoint.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, Integer.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, Long.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, BigInteger.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, BigDecimal.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, String.class);
//                }
//                try ( HdfDataSet dataSet = reader.findDataset("float", channel, reader.getRootGroup()) ) {
//                    HdfTestUtils.displayScalarData(channel, dataSet, HdfFloatPoint.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, Float.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, Double.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, String.class);
//                }
                try ( HdfDataSet dataSet = reader.findDataset("time", channel, reader.getRootGroup()) ) {
                    HdfTestUtils.displayScalarData(channel, dataSet, HdfTime.class);
                    HdfTestUtils.displayScalarData(channel, dataSet, Long.class);
                    HdfTestUtils.displayScalarData(channel, dataSet, BigInteger.class);
                    HdfTestUtils.displayScalarData(channel, dataSet, String.class);
                }
//                try ( HdfDataSet dataSet = reader.findDataset("string", channel, reader.getRootGroup()) ) {
//                    HdfTestUtils.displayScalarData(channel, dataSet, HdfString.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, String.class);
//                }
//                try ( HdfDataSet dataSet = reader.findDataset("compound", channel, reader.getRootGroup()) ) {
//                    HdfTestUtils.displayScalarData(channel, dataSet, HdfCompound.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, String.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, Compound.class);
//                    CompoundDatatype.addConverter(CustomCompound.class, (bytes, compoundDataType)->{
//                        Map<String, HdfCompoundMember> nameToMember = compoundDataType.getInstance(HdfCompound.class, bytes)
//                                .getMembers()
//                                .stream()
//                                .collect(Collectors.toMap(m -> m.getDatatype().getName(), m -> m));
//                        return CustomCompound.builder()
//                                .name("Name")
//                                .someShort(nameToMember.get("a").getInstance(Short.class))
//                                .someDouble(nameToMember.get("b").getInstance(Double.class))
//                                .build();
//                    });
//                    HdfTestUtils.displayScalarData(channel, dataSet, CustomCompound.class);
//                }
//                try ( HdfDataSet dataSet = reader.findDataset("vlen", channel, reader.getRootGroup()) ) {
//                    HdfTestUtils.displayScalarData(channel, dataSet, HdfVariableLength.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, String.class);
//                    HdfTestUtils.displayScalarData(channel, dataSet, Object.class);
//                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        tryHdfApiStrings("string_ascii_all.h5", this::writeAll, StringDatatype.createClassBitField(StringDatatype.PaddingType.SPACE_PAD, StringDatatype.CharacterSet.ASCII), 8);
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

}
