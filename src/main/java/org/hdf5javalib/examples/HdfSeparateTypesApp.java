package org.hdf5javalib.examples;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Objects;

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
            String filePath = Objects.requireNonNull(this.getClass().getResource("/vlen_types_example.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
//                try ( HdfDataSet dataSet = reader.findDataset("fixed_point", channel, reader.getRootGroup()) ) {
//                    displayData(channel, dataSet, HdfFixedPoint.class);
//                    displayData(channel, dataSet, Integer.class);
//                    displayData(channel, dataSet, Long.class);
//                    displayData(channel, dataSet, BigInteger.class);
//                    displayData(channel, dataSet, BigDecimal.class);
//                    displayData(channel, dataSet, String.class);
//                }
//                try ( HdfDataSet dataSet = reader.findDataset("float", channel, reader.getRootGroup()) ) {
//                    displayData(channel, dataSet, HdfFloatPoint.class);
//                    displayData(channel, dataSet, Float.class);
//                    displayData(channel, dataSet, Double.class);
//                    displayData(channel, dataSet, String.class);
//                }
//                try ( HdfDataSet dataSet = reader.findDataset("string", channel, reader.getRootGroup()) ) {
//                    displayData(channel, dataSet, HdfString.class);
//                    displayData(channel, dataSet, String.class);
//                }
//                try ( HdfDataSet dataSet = reader.findDataset("compound", channel, reader.getRootGroup()) ) {
//                    displayData(channel, dataSet, HdfCompound.class);
//                    displayData(channel, dataSet, Compound.class);
//                    displayData(channel, dataSet, String.class);
//                    CompoundDatatype.addConverter(CustomerCompound.class, (bytes, compoundDataType)->{
//                        Map<String, HdfCompoundMember> nameToMember = compoundDataType.getInstance(HdfCompound.class, bytes)
//                                .getMembers()
//                                .stream()
//                                .collect(Collectors.toMap(m -> m.getDatatype().getName(), m -> m));
//                        return CustomerCompound.builder()
//                                .name("Name")
//                                .someShort(nameToMember.get("a").getInstance(Short.class))
//                                .someDouble(nameToMember.get("b").getInstance(Double.class))
//                                .build();
//                    });
//                    displayData(channel, dataSet, CustomerCompound.class);
//                }
                try ( HdfDataSet dataSet = reader.findDataset("vlen_int", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfVariableLength.class);
                    displayData(channel, dataSet, String.class);
                    displayData(channel, dataSet, Object.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("vlen_float", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfVariableLength.class);
                    displayData(channel, dataSet, String.class);
                    displayData(channel, dataSet, Object.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("vlen_double", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfVariableLength.class);
                    displayData(channel, dataSet, String.class);
                    displayData(channel, dataSet, Object.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("vlen_string", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfVariableLength.class);
                    displayData(channel, dataSet, String.class);
                    displayData(channel, dataSet, Object.class);
                }
                try ( HdfDataSet dataSet = reader.findDataset("vlen_short", channel, reader.getRootGroup()) ) {
                    displayData(channel, dataSet, HdfVariableLength.class);
                    displayData(channel, dataSet, String.class);
                    displayData(channel, dataSet, Object.class);
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

        T result = dataSource.readScalar();
        System.out.println(clazz.getSimpleName() + " read = " + displayValue(result));

        result = dataSource.streamScalar().findFirst().orElseThrow();
        System.out.println(clazz.getSimpleName() + " stream = " + displayValue(result));
    }

    private String displayValue(Object value) {
        if (value == null) return "null";
        Class<?> clazz = value.getClass();
        if (!clazz.isArray()) return value.toString();

        if (clazz == int[].class) return Arrays.toString((int[]) value);
        if (clazz == float[].class) return Arrays.toString((float[]) value);
        if (clazz == double[].class) return Arrays.toString((double[]) value);
        if (clazz == long[].class) return Arrays.toString((long[]) value);
        if (clazz == short[].class) return Arrays.toString((short[]) value);
        if (clazz == byte[].class) return Arrays.toString((byte[]) value);
        if (clazz == char[].class) return Arrays.toString((char[]) value);
        if (clazz == boolean[].class) return Arrays.toString((boolean[]) value);

        return Arrays.deepToString((Object[]) value); // For Object[] or nested Object[][]
    }
}
