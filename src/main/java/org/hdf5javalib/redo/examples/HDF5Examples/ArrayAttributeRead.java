package org.hdf5javalib.redo.examples.HDF5Examples;

import org.hdf5javalib.redo.HdfFileReader;
import org.hdf5javalib.redo.dataclass.*;
import org.hdf5javalib.redo.datasource.TypedDataSource;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataSet;
import org.hdf5javalib.redo.utils.HdfDataHolder;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;

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
                log.debug("Root Group: {} ", reader.getRootGroup());
                reader.getFileAllocation().printBlocks();
//                try (HdfDataSet dataSet = reader.getRootGroup().getDataset("/DS1").orElseThrow()) {
//                    displayData(channel, dataSet, reader);
//                }
                for(HdfDataSet dataSet: reader.getRootGroup().getDataSets()) {
                    displayAttributes(dataSet);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void displayAttributes(HdfDataSet dataSet) {
        dataSet.getAttributeMessages().forEach(message -> {
            // iterator
            HdfDataHolder dataHolder = message.getHdfDataHolder();
            dataHolder.iterator().forEachRemaining(System.out::println);
            // individual elements of a matrix
            int[] dimensions = dataHolder.getDimensions();
            for(int i=0; i < dimensions[0]; ++i) {
                HdfData row = dataHolder.get(i);
                HdfData[] columns = row.getInstance(HdfData[].class);
                for(int j=0; j < columns.length; ++j) {
                    System.out.print(columns[j] + " ");
                }
                System.out.println();
            }
            // get all
            HdfData[] data = dataHolder.getAll(HdfData[].class);
            System.out.println("Data = " + Arrays.toString(data));
        });
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