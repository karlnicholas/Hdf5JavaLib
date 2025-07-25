package org.hdf5javalib.examples.h5ex_t;

import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.hdf5javalib.utils.HdfDisplayUtils.displayData;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class h5ex_t_read {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(h5ex_t_read.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new h5ex_t_read().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {

            // List all .h5 files in HDF5Examples resources directory
            Path dirPath = Paths.get(h5ex_t_read.class.getClassLoader().getResource("h5ex_t").toURI());
            Files.list(dirPath)
                    .filter(p -> p.toString().endsWith(".h5"))
                    .forEach(p -> {
                        System.out.println("Running " + p.getFileName());
                        displayFile(p);
                    });
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void displayFile(Path filePath) {
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            for (HdfDataset dataSet : reader.getDatasets()) {
                log.info("{} ", dataSet);
                displayData(channel, dataSet, reader);
            }
        } catch (Exception e) {
            log.error("Exception in displayFile: {}", filePath, e);
        }
    }
}