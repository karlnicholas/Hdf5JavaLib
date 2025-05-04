package org.hdf5javalib.examples.read;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Demonstrates reading string datasets from HDF5 files.
 * <p>
 * The {@code HdfStringRead} class is an example application that reads string
 * datasets from two HDF5 files, one encoded in ASCII and the other in UTF-8.
 * It uses {@link HdfDisplayUtils} to display the vector data from the datasets.
 * </p>
 */
public class HdfStringRead {
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HdfStringRead().run();
    }

    /**
     * Executes the main logic of reading and displaying string datasets from HDF5 files.
     */
    private void run() {
        try {
            String filePath = Objects.requireNonNull(HdfStringRead.class.getResource("/ascii_dataset.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try (HdfDataSet dataSet = reader.getRootGroup().findDataset("strings")) {
                    HdfDisplayUtils.displayVectorData(channel, dataSet, String.class, reader);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            String filePath = Objects.requireNonNull(HdfStringRead.class.getResource("/utf8_dataset.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try (HdfDataSet dataSet = reader.getRootGroup().findDataset("strings")) {
                    HdfDisplayUtils.displayVectorData(channel, dataSet, String.class, reader);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}