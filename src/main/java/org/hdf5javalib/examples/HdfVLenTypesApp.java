package org.hdf5javalib.examples;

import lombok.Builder;
import lombok.Data;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.HdfDisplayUtils;

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
            String filePath = Objects.requireNonNull(this.getClass().getResource("/vlen_types_example.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                for ( HdfDataSet dataSet: reader.getRootGroup().getDataSets() ) {
                    try (HdfDataSet ds = dataSet) {
                        System.out.println();
                        System.out.println("Dataset name: " + ds.getDatasetName());
                        HdfDisplayUtils.displayScalarData(channel, ds, HdfVariableLength.class, reader);
                        HdfDisplayUtils.displayScalarData(channel, ds, String.class, reader);
                        HdfDisplayUtils.displayScalarData(channel, ds, Object.class, reader);
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
