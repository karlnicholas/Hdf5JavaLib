package org.hdf5javalib.redo.examples.read;

import org.hdf5javalib.redo.HdfFileReader;
import org.hdf5javalib.redo.dataclass.HdfVariableLength;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfDataSet;
import org.hdf5javalib.redo.utils.HdfDisplayUtils;

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
            for (HdfDataSet dataSet : reader.getRootGroup().getDataSets()) {
                try (HdfDataSet ds = dataSet) {
                    System.out.println();
                    System.out.println("Dataset name: " + ds.getDatasetName());
                    HdfDisplayUtils.displayScalarData(channel, ds, HdfVariableLength.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, ds, String.class, reader);
                    HdfDisplayUtils.displayScalarData(channel, ds, Object.class, reader);
                }
            }
            log.info("Superblock: {}", reader.getFileAllocation().getSuperblock());
            reader.getFileAllocation().printBlocks();
        }
    }

    /**
     * A data class representing a basic compound dataset with two fields.
     */
    public static class Compound {
        /** A short integer field. */
        private Short a;
        /** A double-precision floating-point field. */
        private Double b;

        public Short getA() {
            return a;
        }

        public void setA(Short a) {
            this.a = a;
        }

        public Double getB() {
            return b;
        }

        public void setB(Double b) {
            this.b = b;
        }
    }

    /**
     * A custom data class for a compound dataset with renamed fields.
     */
    public static class CustomCompound {
        /** A name identifier for the compound. */
        private String name;
        /** A short integer field. */
        private Short someShort;
        /** A double-precision floating-point field. */
        private Double someDouble;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Short getSomeShort() {
            return someShort;
        }

        public void setSomeShort(Short someShort) {
            this.someShort = someShort;
        }

        public Double getSomeDouble() {
            return someDouble;
        }

        public void setSomeDouble(Double someDouble) {
            this.someDouble = someDouble;
        }
    }
}