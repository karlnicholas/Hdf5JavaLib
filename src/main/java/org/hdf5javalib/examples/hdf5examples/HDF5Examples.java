package org.hdf5javalib.examples.hdf5examples;

import org.hdf5javalib.datasource.TypedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hdf5javalib.utils.HdfDisplayUtils.displayFile;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class HDF5Examples {
    private static final Logger log = LoggerFactory.getLogger(HDF5Examples.class);
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HDF5Examples().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            // List all .h5 files in HDF5Examples resources directory
            Path dirPath = Paths.get(Objects.requireNonNull(HDF5Examples.class.getClassLoader().getResource("HDF5Examples")).toURI());
            try ( Stream<Path> streamList = Files.list(dirPath) ) {
                streamList.filter(p -> p.toString().endsWith(".h5"))
                        .forEach(p -> {
                            log.info("Running {}", p.getFileName());
                            displayFile(p);
                        });

            }
        } catch (URISyntaxException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

}