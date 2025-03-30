package org.hdf5javalib.examples;

import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfFloatPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
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

    private <T> void displayData(FileChannel fileChannel, HdfDataSet dataSet, Class<T> clazz) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(dataSet, fileChannel, clazz);
        System.out.println(clazz.getSimpleName() + " read = " + dataSource.readScalar());
        System.out.println(clazz.getSimpleName() + " stream = " + dataSource.streamScalar().findFirst().orElseThrow());
    }

}
