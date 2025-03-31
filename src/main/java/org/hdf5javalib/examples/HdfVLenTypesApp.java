package org.hdf5javalib.examples;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.*;
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
public class HdfVLenTypesApp {
    public static void main(String[] args) {
        new HdfVLenTypesApp().run();
    }

    private void run() {
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = Objects.requireNonNull(this.getClass().getResource("/vlen_types_example.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                for ( HdfDataSet dataSet: reader.getDatasets(channel, reader.getRootGroup()) ) {
                    System.out.println();
                    System.out.println("Dataset name: " + dataSet.getDatasetName());
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
    public static class CustomCompound {
        private String name;
        private Short someShort;
        private Double someDouble;
    }

    private <T> void displayData(FileChannel fileChannel, HdfDataSet dataSet, Class<T> clazz) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(dataSet, fileChannel, clazz);

        T result = dataSource.readScalar();
        System.out.println(displayType(clazz, result) + " read = " + displayValue(result));

        result = dataSource.streamScalar().findFirst().orElseThrow();
        System.out.println(displayType(clazz, result) + " stream = " + displayValue(result));
    }

    private String displayType(Class<?> declaredType, Object actualValue) {
        if (actualValue == null) return declaredType.getSimpleName();
        Class<?> actualClass = actualValue.getClass();
        if (actualClass.isArray()) {
            Class<?> componentType = actualClass.getComponentType();
            return declaredType.getSimpleName() + "(" + componentType.getSimpleName() + "[])";
        }
        return declaredType.getSimpleName();
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
