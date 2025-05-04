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
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code HdfCompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
@Slf4j
public class HdfCompoundRead {
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HdfCompoundRead().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            Path filePath = getResourcePath("compound_example.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try (HdfDataSet dataSet = reader.getRootGroup().findDataset("CompoundData")) {
                    displayData(channel, dataSet, reader);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves the file path for a resource.
     *
     * @param fileName the name of the resource file
     * @return the Path to the resource file
     */
    private Path getResourcePath(String fileName) {
        String resourcePath = getClass().getClassLoader().getResource(fileName).getPath();
        if (System.getProperty("os.name").toLowerCase().contains("windows") && resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return Paths.get(resourcePath);
    }

    /**
     * A data class representing a compound dataset record.
     */
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CompoundExample {
        /** The record ID. */
        private Long recordId;
        /** A fixed-length string. */
        private String fixedStr;
        /** A variable-length string. */
        private String varStr;
        /** A float value. */
        private Float floatVal;
        /** A double value. */
        private Double doubleVal;
        /** An 8-bit integer value. */
        private Byte int8_Val;
        /** A 16-bit integer value. */
        private Short int16_Val;
        /** A 32-bit integer value. */
        private Integer int32_Val;
        /** A 64-bit integer value. */
        private Long int64_Val;
        /** An unsigned 8-bit integer value. */
        private Short uint8_Val;
        /** An unsigned 16-bit integer value. */
        private Integer uint16_Val;
        /** An unsigned 32-bit integer value. */
        private Long uint32_Val;
        /** An unsigned 64-bit integer value. */
        private BigInteger uint64_Val;
        /** A scaled unsigned integer value as a BigDecimal. */
        private BigDecimal scaledUintVal;
    }

    /**
     * Displays data from a compound dataset using a TypedDataSource.
     * <p>
     * Reads the dataset, processes it as both raw HdfCompound and custom CompoundExample
     * objects, and prints selected rows and filtered values.
     * </p>
     *
     * @param seekableByteChannel the file channel for reading the HDF5 file
     * @param dataSet             the compound dataset to process
     * @param hdfDataFile         the HDF5 file context
     * @throws IOException if an I/O error occurs
     */
    public void displayData(SeekableByteChannel seekableByteChannel, HdfDataSet dataSet, HdfDataFile hdfDataFile) throws IOException {
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