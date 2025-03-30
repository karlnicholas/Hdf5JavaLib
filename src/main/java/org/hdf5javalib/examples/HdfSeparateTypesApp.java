package org.hdf5javalib.examples;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.*;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.util.List;
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
                try ( HdfDataSet dataSet = reader.findDataset("fixed_point", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfFixedPoint.class);
                    displayData(channel, dataSet, Integer.class);
                    displayData(channel, dataSet, Long.class);
                    displayData(channel, dataSet, BigInteger.class);
                    displayData(channel, dataSet, BigDecimal.class);
                    displayData(channel, dataSet, String.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("float", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfFloatPoint.class);
                    displayData(channel, dataSet, Float.class);
                    displayData(channel, dataSet, Double.class);
                    displayData(channel, dataSet, String.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("string", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfString.class);
                    displayData(channel, dataSet, String.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("compound", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfCompound.class);
                    displayData(channel, dataSet, Compound.class);
                    displayData(channel, dataSet, String.class);
                    CompoundDatatype.addConverter(CustomerCompound.class, (bytes, compoundDataType)->{
                        Map<String, HdfCompoundMember> nameToMember = compoundDataType.getInstance(HdfCompound.class, bytes)
                                .getMembers()
                                .stream()
                                .collect(Collectors.toMap(m -> m.getDatatype().getName(), m -> m));
                        return CustomerCompound.builder()
                                .name("Name")
                                .someShort(nameToMember.get("a").getInstance(Short.class))
                                .someDouble(nameToMember.get("b").getInstance(Double.class))
                                .build();
                    });
                    displayData(channel, dataSet, CustomerCompound.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("vlen", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfVariableLength.class);
                    displayData(channel, dataSet, String.class);
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
    public static class CustomerCompound {
        private String name;
        private Short someShort;
        private Double someDouble;
    }


    private <T> void displayData(FileChannel fileChannel, HdfDataSet dataSet, Class<T> clazz) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(dataSet, fileChannel, clazz);
        System.out.println(clazz.getSimpleName() + " read = " + dataSource.readScalar());
        System.out.println(clazz.getSimpleName() + " stream = " + dataSource.streamScalar().findFirst().orElseThrow());
    }

}
