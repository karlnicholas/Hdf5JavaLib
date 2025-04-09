package org.hdf5javalib.examples;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.HdfTestUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
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
                    try (HdfDataSet ds = dataSet) {
                        System.out.println();
                        System.out.println("Dataset name: " + ds.getDatasetName());
                        HdfTestUtils.displayScalarData(channel, ds, HdfVariableLength.class, reader);
                        HdfTestUtils.displayScalarData(channel, ds, String.class, reader);
                        HdfTestUtils.displayScalarData(channel, ds, Object.class, reader);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
