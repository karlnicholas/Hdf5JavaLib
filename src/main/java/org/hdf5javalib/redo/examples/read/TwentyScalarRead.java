package org.hdf5javalib.redo.examples.read;

import org.hdf5javalib.redo.HdfFileReader;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfDataSet;
import org.hdf5javalib.redo.utils.HdfDisplayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hdf5javalib.redo.utils.HdfReadUtils.getResourcePath;

/**
 * Demonstrates reading scalar datasets from an HDF5 file.
 * <p>
 * The {@code TwentyScalarRead} class is an example application that reads
 * multiple scalar datasets from an HDF5 file and displays their values as
 * {@code Long} using {@link HdfDisplayUtils}.
 * </p>
 */
public class TwentyScalarRead {
    private static final Logger log = LoggerFactory.getLogger(TwentyScalarRead.class);
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) throws Exception {
        new TwentyScalarRead().run();
    }

    /**
     * Executes the main logic of reading and displaying scalar datasets from an HDF5 file.
     */
    private void run() throws Exception {
        Path filePath = getResourcePath("twenty_datasets.h5");
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            for (HdfDataSet dataSet : reader.getRootGroup().getDataSets()) {
                try (HdfDataSet ds = dataSet) {
                    HdfDisplayUtils.displayScalarData(channel, ds, Long.class, reader);
                }
            }
            log.debug("Root Group: {} ", reader.getRootGroup());
            reader.getFileAllocation().printBlocks();
//                try (HdfDataset ds = reader.getRootGroup().findDataset("dataset_14")) {
//                    HdfDisplayUtils.displayScalarData(channel, ds, Long.class, reader);
//                }
        }
    }
}