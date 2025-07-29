package org.hdf5javalib.examples.read;

import org.hdf5javalib.dataclass.*;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class CompoundRead {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(CompoundRead.class);

    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new CompoundRead().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            Path filePath = getResourcePath("compound_example.h5");
            try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try (HdfDataset dataSet = reader.getDataset("/CompoundData").orElseThrow()) {
                    displayData(channel, dataSet, reader);
                }
                log.debug("File BTree: {} ", reader.getBTree());
                }

        } catch (Exception e) {
            throw new IllegalStateException(e);
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

    /**
     * Displays data from a compound dataset using a TypedDataSource.
     * <p>
     * Reads the dataset, processes it as both raw HdfCompound and custom CompoundExample
     * objects, and prints selected rows and filtered values.
     * </p>
     *
     * @param seekableByteChannel the file channel for reading the HDF5 file
     * @param dataSet             the compound dataset to process
     * @param hdfDataFile         the HDF5 file context
     * @throws IOException if an I/O error occurs
     */
    public void displayData(SeekableByteChannel seekableByteChannel, HdfDataset dataSet, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        System.out.println("Ten Rows:");
        new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, HdfCompound.class)
                .streamVector()
                .limit(10)
                .forEach(c -> System.out.println("Row: " + c.getMembers()));

//        System.out.println("Ten BigDecimals = " + new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, HdfCompound.class).streamVector()
//                .filter(c -> c.getMembers().get(0).getInstance(Long.class) < 1010)
//                .map(c -> c.getMembers().get(13).getInstance(BigDecimal.class)).toList());
//
//        System.out.println("Custom record class:");
//        new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, Record.class)
//                .streamVector()
//                .forEach(c -> System.out.println("Row: " + c));
//        System.out.println("DONE");
        AtomicInteger atomicInteger = new AtomicInteger(0);
        new TypedDataSource<>(seekableByteChannel, hdfDataFile, dataSet, HdfCompound.class)
                .streamVector()
                .forEach(c-> {
                    c.getMembers().toString();
                    atomicInteger.incrementAndGet();
                });
        System.out.println("DONE: " + atomicInteger.get());
    }
}