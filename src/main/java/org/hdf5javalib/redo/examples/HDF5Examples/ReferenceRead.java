package org.hdf5javalib.redo.examples.HDF5Examples;

import org.hdf5javalib.redo.HdfFileReader;
import org.hdf5javalib.redo.datasource.TypedDataSource;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataSet;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.hdf5javalib.redo.utils.HdfDisplayUtils.displayData;
import static org.hdf5javalib.redo.utils.HdfReadUtils.getResourcePath;

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
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ReferenceRead.class);

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
        try {
            Path filePath = getResourcePath("HDF5Examples/h5ex_t_cpxcmpd.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                log.debug("Root Group: {} ", reader.getRootGroup());
                reader.getFileAllocation().printBlocks();
//                try (HdfDataSet dataSet = reader.getRootGroup().getDataset("/DS1").orElseThrow()) {
//                    displayReference(channel, dataSet, reader);
//                }
                for (HdfDataSet dataSet : reader.getRootGroup().getDataSets()) {
                    displayData(channel, dataSet, reader);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void displayReference(SeekableByteChannel channel, HdfDataSet dataSet, HdfFileReader reader) {
        dataSet.getReferenceInstances().forEach(referenceInstance -> {
            System.out.println("referenceInstance=" + referenceInstance);
        });
    }


}