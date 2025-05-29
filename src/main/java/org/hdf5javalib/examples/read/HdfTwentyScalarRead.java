package org.hdf5javalib.examples.read;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Demonstrates reading scalar datasets from an HDF5 file.
 * <p>
 * The {@code TwentyScalarRead} class is an example application that reads
 * multiple scalar datasets from an HDF5 file and displays their values as
 * {@code Long} using {@link HdfDisplayUtils}.
 * </p>
 */
public class HdfTwentyScalarRead {
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HdfTwentyScalarRead().run();
    }

    /**
     * Executes the main logic of reading and displaying scalar datasets from an HDF5 file.
     */
    private void run() {
        try {
            Path filePath = getResourcePath("twenty_datasets.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                for (HdfDataSet dataSet : reader.getRootGroup().getDataSets()) {
                    try (HdfDataSet ds = dataSet) {
                        HdfDisplayUtils.displayScalarData(channel, ds, Long.class, reader);
                    }
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
}