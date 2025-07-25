package org.hdf5javalib.examples.HDF5Examples;

import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hdf5javalib.utils.HdfDisplayUtils.displayData;
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
public class ReferenceRead {
    private static final Logger log = LoggerFactory.getLogger(ReferenceRead.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new ReferenceRead().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
//        try {
////            Path filePath = getResourcePath("HDF5Examples/h5ex_t_cpxcmpd.h5");
//            Path filePath = getResourcePath("HDF5Examples/h5ex_d_sofloat.h5");
//            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
//                HdfFileReader reader = new HdfFileReader(channel).readFile();
////                try (HdfDataset dataSet = reader.getRootGroup().getDataset("/DS1").orElseThrow()) {
////                    displayReference(channel, dataSet, reader);
////                }
//                for (HdfDataset dataSet : reader.getDatasets()) {
//                    log.debug("dataSet: {} ", dataSet);
//                    displayData(channel, dataSet, reader);
//                }
//                log.debug("Superblock: {} ", reader.getSuperblock());
//            }
//        } catch (Exception e) {
//            log.error(e.getMessage(), e);
//        }
        try {
//            Path filePath = getResourcePath("HDF5Examples/h5ex_t_cpxcmpd.h5");
            Path filePath = getResourcePath("HDF5Examples/h5ex_t_cpxcmpd.h5");
            System.out.println(filePath.toFile().getAbsolutePath());
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
//                try (HdfDataset dataSet = reader.getRootGroup().getDataset("/DS1").orElseThrow()) {
//                    displayReference(channel, dataSet, reader);
//                }
                for (HdfDataset dataSet : reader.getDatasets()) {
                    log.debug("dataSet: {} ", dataSet);
                    displayData(channel, dataSet, reader);
                }
                log.debug("Superblock: {} ", reader.getSuperblock());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void displayReference(SeekableByteChannel channel, HdfDataset dataSet, HdfFileReader reader) {
        dataSet.getReferenceInstances().forEach(referenceInstance -> {
            System.out.println("referenceInstance=" + referenceInstance);
        });
    }


}