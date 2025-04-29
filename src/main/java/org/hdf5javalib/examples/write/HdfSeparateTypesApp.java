package org.hdf5javalib.examples.write;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.*;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.util.BitSet;
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
            String filePath = Objects.requireNonNull(this.getClass().getResource("/all_types_separate.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("fixed_point") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfFixedPoint.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, Integer.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, Long.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, BigInteger.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, BigDecimal.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("float") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfFloatPoint.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, Float.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, Double.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("time") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfTime.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, Long.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, BigInteger.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("string") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfString.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("bitfield") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfBitField.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, BitSet.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("compound") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfCompound.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, Compound.class, reader);
                    CompoundDatatype.addConverter(CustomCompound.class, (bytes, compoundDataType)->{
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
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("opaque") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfOpaque.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, byte[].class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("reference") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfReference.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, byte[].class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("enum") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfEnum.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("vlen") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfVariableLength.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, Object.class, reader);
                }
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("array") ) {
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfArray.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, String.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, dataSet, HdfData[].class, reader);
                }
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
