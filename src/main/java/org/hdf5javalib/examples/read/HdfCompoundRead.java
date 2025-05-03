package org.hdf5javalib.examples.read;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Hello world!
 */
@Slf4j
public class HdfCompoundRead {
    public static void main(String[] args) {
        new HdfCompoundRead().run();
    }

    private void run() {
        try {
            Path filePath = getResourcePath("compound_example.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try (HdfDataSet dataSet = reader.getRootGroup().findDataset("CompoundData")) {
                    displayData(channel, dataSet, reader);
                }
//                reader.getGlobalHeap().printDebug();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CompoundExample {
        private Long recordId;
        private String fixedStr;
        private String varStr;
        private Float floatVal;
        private Double doubleVal;
        private Byte int8_Val;
        private Short int16_Val;
        private Integer int32_Val;
        private Long int64_Val;
        private Short uint8_Val;
        private Integer uint16_Val;
        private Long uint32_Val;
        private BigInteger uint64_Val;
        private BigDecimal scaledUintVal;
    }

    public void displayData(SeekableByteChannel seekableByteChannel, HdfDataSet dataSet, HdfDataFile hdfDataFile) throws IOException {
//        System.out.println("Count = " + new TypedDataSource<>(dataSet, fileChannel, HdfCompound.class).streamVector().count());

        System.out.println("Ten Rows:");
        new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, HdfCompound.class)
                .streamVector()
                .limit(10)
                .forEach(c -> System.out.println("Row: " + c.getMembers()));

        System.out.println("Ten BigDecimals = " + new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, HdfCompound.class).streamVector()
                .filter(c -> c.getMembers().get(0).getInstance(Long.class) < 1010)
                .map(c -> c.getMembers().get(13).getInstance(BigDecimal.class)).toList());

        System.out.println("RecordId < 1010, custom class:");
        new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, CompoundExample.class)
                .streamVector()
                .filter(c -> c.getRecordId() < 1010)
                .forEach(c -> System.out.println("Row: " + c));
        System.out.println("DONE");
    }
}
