package org.hdf5javalib.examples.hdf5examples;

import org.hdf5javalib.datasource.TypedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

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
public class HDF5Debug {
    private static final Logger log = LoggerFactory.getLogger(HDF5Debug.class);
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HDF5Debug().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            // List all .h5 files in HDF5Examples resources directory
            // ATL03_20250302235544_11742607_006_01
//            Path dirPath = Paths.get(Objects.requireNonNull(HDF5Debug.class.getClassLoader().getResource("HDF5Examples/h5ex_g_compact2.h5")).toURI());
            Path dirPath = Paths.get("c:/users/karln/Downloads/ATL03_20250302235544_11742607_006_01.h5");
            displayFile(dirPath);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}