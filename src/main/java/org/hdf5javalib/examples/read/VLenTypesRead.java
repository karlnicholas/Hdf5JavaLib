package org.hdf5javalib.examples.read;

import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.FileInputStream;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Demonstrates reading variable-length data types from an HDF5 file.
 * <p>
 * The {@code HdfVLenTypesRead} class is an example application that reads
 * variable-length datasets from an HDF5 file and displays their contents
 * using {@link HdfDisplayUtils}. It processes the data as {@link HdfVariableLength},
 * {@code String}, and generic {@code Object} types.
 * </p>
 */
public class VLenTypesRead {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(VLenTypesRead.class);
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) throws Exception {
        new VLenTypesRead().run();
    }

    /**
     * Executes the main logic of reading and displaying variable-length datasets from an HDF5 file.
     */
    private void run() throws Exception {
        String filePath = Objects.requireNonNull(this.getClass().getResource("/vlen_types_example.h5")).getFile();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            FileChannel channel = fis.getChannel();
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            for (HdfDataset dataSet : reader.getDatasets()) {
                try (HdfDataset ds = dataSet) {
                    System.out.println();
                    System.out.println("Dataset name: " + ds.getObjectName());
                    HdfDisplayUtils.displayScalarData(channel, ds, HdfVariableLength.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, ds, String.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, ds, Object.class, reader);
                }
            }
            log.info("Superblock: {}", reader.getSuperblock());
        }
    }
}