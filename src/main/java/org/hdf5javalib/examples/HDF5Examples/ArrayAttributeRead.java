package org.hdf5javalib.examples.HDF5Examples;

import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hdf5javalib.utils.HdfDisplayUtils.displayAttributes;
import static org.hdf5javalib.utils.HdfReadUtils.getResourcePath;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class ArrayAttributeRead {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ArrayAttributeRead.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new ArrayAttributeRead().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            Path filePath = getResourcePath("HDF5Examples/h5ex_t_arrayatt.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                log.debug("BTree: {} ", reader.getBTree().getRoot());
                for (HdfDataset dataSet : reader.getDatasets()) {
                    displayAttributes(dataSet);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}