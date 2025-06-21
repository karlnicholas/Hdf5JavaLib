package org.hdf5javalib.redo.examples.HDF5Examples;

import org.hdf5javalib.redo.HdfFileReader;
import org.hdf5javalib.redo.datasource.TypedDataSource;
import org.hdf5javalib.redo.hdffile.HdfDataSet;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.hdf5javalib.redo.utils.HdfDisplayUtils.displayData;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class ExampleDebug {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ExampleDebug.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new ExampleDebug().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            Path filePath = Paths.get("c:/users/karln/downloads/ATL03_20250302235544_11742607_006_01.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                log.debug("Root Group: {} ", reader.getRootGroup());
                reader.getFileAllocation().printBlocks();
//                try (HdfDataSet dataSet = reader.getRootGroup().getDataset("/DS1").orElseThrow()) {
//                    displayData(channel, dataSet, reader);
//                }
                for (HdfDataSet dataSet : reader.getRootGroup().getDataSets()) {
                    displayData(channel, dataSet, reader);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}