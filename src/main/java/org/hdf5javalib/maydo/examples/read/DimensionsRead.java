package org.hdf5javalib.maydo.examples.read;

import org.hdf5javalib.maydo.HdfFileReader;
import org.hdf5javalib.maydo.dataclass.HdfData;
import org.hdf5javalib.maydo.datasource.TypedDataSource;
import org.hdf5javalib.maydo.hdfjava.HdfDataset;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hdf5javalib.maydo.utils.HdfDisplayUtils.displayScalarData;
import static org.hdf5javalib.maydo.utils.HdfReadUtils.getResourcePath;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class DimensionsRead {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DimensionsRead.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new DimensionsRead().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            Path filePath = getResourcePath("dimensions.h5");
//            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
//                HdfFileReader reader = new HdfFileReader(channel).readFile();
//                log.debug("Root Group: {} ", reader.getRootGroup());
//                try (HdfDataset ds = reader.getRootGroup().getDataset("/scalar_dataset").orElseThrow()) {
//                    displayScalarData(channel, ds, HdfFloatPoint.class, reader);
//                }
//                try (HdfDataset ds = reader.getRootGroup().getDataset("/1d_dataset").orElseThrow()) {
//                    displayVectorData(channel, ds, HdfFloatPoint.class, reader);
//                }
//                try (HdfDataset ds = reader.getRootGroup().getDataset("/2d_dataset").orElseThrow()) {
//                    displayMatrixData(channel, ds, HdfFloatPoint.class, reader);
//                }
//                try (HdfDataset ds = reader.getRootGroup().getDataset("/2d_dataset_permuted").orElseThrow()) {
//                    displayMatrixData(channel, ds, HdfFloatPoint.class, reader);
//                }
//            }
            filePath = getResourcePath("array_datasets.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                log.debug("Root Group: {} ", reader.getRootGroup());
                for (HdfDataset ds : reader.getRootGroup().getDataSets()) {
                    displayScalarData(channel, ds, HdfData[].class, reader);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}