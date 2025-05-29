package org.hdf5javalib.redo.examples.HDF5Examples;

import org.hdf5javalib.redo.HdfFileReader;
import org.hdf5javalib.redo.dataclass.*;
import org.hdf5javalib.redo.datasource.TypedDataSource;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataSet;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;

import static org.hdf5javalib.redo.utils.HdfDisplayUtils.*;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class ExamplesRead {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ExamplesRead.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new ExamplesRead().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {

            // List all .h5 files in HDF5Examples resources directory
            Path dirPath = Paths.get(ExamplesRead.class.getClassLoader().getResource("HDF5Examples").toURI());
            Files.list(dirPath)
                    .filter(p -> p.toString().endsWith(".h5"))
                    .forEach(p -> {
                        try {
                            System.out.println("Running " + p.getFileName());
                            printFile(p);
                        } catch (Exception e){
                            e.printStackTrace();
//                            throw new RuntimeException(e);
                        }});
        } catch (URISyntaxException e) {
                throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void printFile(Path filePath) throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            log.debug("Root Group: {} ", reader.getRootGroup());
            for (HdfDataSet ds : reader.getRootGroup().getDataSets()) {
                System.out.println(ds.getDatasetName());
                switch (ds.getDimensionality()) {
                    case 0:
                        displayScalarData(channel, ds, HdfData.class, reader);
                        break;
                    case 1:
                        displayVectorData(channel, ds, HdfData.class, reader);
                        break;
                    case 2:
                        displayMatrixData(channel, ds, HdfData.class, reader);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + ds.getDimensionality());

                }
            }
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

    public record Record(
            Integer fixed_point,              // int32_t fixed_point
            Float floating_point,         // float floating_point
            Long time,                   // uint64_t time (Class 2 Time)
            String string,               // char string[16]
            BitSet bit_field,               // uint8_t bit_field
            HdfOpaque opaque,               // uint8_t opaque[4]
            Compound compound,           // nested struct compound
            HdfReference reference,              // hobj_ref_t reference
            HdfEnum enumerated,            // int enumerated (LOW, MEDIUM, HIGH)
            HdfArray array,                 // int array[3]
            HdfVariableLength variable_length         // hvl_t variable_length
    ) {
        // Nested record for compound
        public record Compound(
                Integer nested_int,          // int16_t nested_int
                Double nested_double      // double nested_double
        ) {
        }

        // Enum for enumerated field
        public enum Level {
            LOW(0), MEDIUM(1), HIGH(2);
            private final int value;

            Level(int value) {
                this.value = value;
            }

            public int getValue() {
                return value;
            }
        }

//        // Canonical constructor for validation
//        public Record {
//            if (string == null || string.length() > 16) {
//                throw new IllegalArgumentException("string must be non-null and at most 16 characters");
//            }
//            // Pad string to 16 chars with NULs for HDF5
//            string = string.length() < 16 ? string + "\0".repeat(16 - string.length()) : string;
//            if (opaque == null || opaque.length != 4) {
//                throw new IllegalArgumentException("opaque must be a 4-byte array");
//            }
//            if (array == null || array.length != 3) {
//                throw new IllegalArgumentException("array must be a 3-element int array");
//            }
//            if (variableLength == null) {
//                throw new IllegalArgumentException("variableLength must be non-null");
//            }
//        }
    }

}