package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Utility class for displaying HDF5 dataset data and managing attributes.
 * <p>
 * The {@code HdfDisplayUtils} class provides methods to display scalar and vector data
 * from HDF5 datasets using a {@link TypedDataSource}, as well as to create version attributes
 * for datasets. It supports various data types and formats the output for easy inspection,
 * handling both primitive and array types.
 * </p>
 */
public class HdfDisplayUtils {
    private static final Logger log = LoggerFactory.getLogger(HdfDisplayUtils.class);
    public static final String UNDEFINED = "<Undefined>";
    private static final String READ_EQUALS = " read = ";
    private static final String STREAM_EQUALS = " stream = ";

    // Define a functional interface for actions that may need channel, dataset, and reader
    @FunctionalInterface
    interface FileAction {
        void perform(SeekableByteChannel channel, HdfDataset dataSet, HdfFileReader reader) throws Exception;
    }

    public static String undefinedArrayToString(HdfFixedPoint[] values) {
        if (values == null || values.length == 0) {
            return "Not Present";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            sb.append(values[i].isUndefined()?UNDEFINED:values[i].toString() );
            if (i != values.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(']');
        return sb.toString();
    }

    // Generalized method to process the file and apply a custom action per dataset
    private static void processFile(Path filePath, FileAction action) {
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            for (HdfDataset dataSet : reader.getDatasets()) {
                log.info("{} ", dataSet);
                action.perform(channel, dataSet, reader);
            }
        } catch (Exception e) {
            log.error("Exception in processFile: {}", filePath, e);
        }
    }

    public static void displayFileAttr(Path filePath) {
        processFile(filePath, (channel, dataSet, reader) -> displayAttributes(dataSet));
    }

    public static void displayFile(Path filePath) {
        processFile(filePath, HdfDisplayUtils::displayData);
    }

    public static void displayAttributes(HdfDataset dataSet) {
        dataSet.getAttributeMessages().forEach(message -> {
            HdfDataHolder dataHolder = message.getHdfDataHolder();
            if (dataHolder.getDimensionality() == 1) {
                HdfData[] data = dataHolder.getAll(HdfData[].class);
                System.out.println("Data = " + Arrays.toString(data));
            } else if (dataHolder.getDimensionality() == 2) {
                HdfData[][] data = dataHolder.getAll(HdfData[][].class);
                for ( HdfData[] row : data) {
                    System.out.println("Row = " + Arrays.toString(row));
                }
            }
        });
    }

    public static void displayData(SeekableByteChannel channel, HdfDataset ds, HdfFileReader reader) throws Exception {
        log.debug("Dataset path: {}", ds.getObjectPath());
        if (ds.hasData()) {
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
                    displayNDimData(channel, ds, HdfData.class, reader);
                    break;

            }
        }
    }

    /**
     * Displays scalar data from a dataset.
     * <p>
     * Reads and prints the scalar value from the dataset using both direct reading and
     * streaming methods, formatting the output with the dataset name and type information.
     * </p>
     *
     * @param fileChannel the seekable byte channel for reading the HDF5 file
     * @param dataSet     the dataset to read from
     * @param clazz       the class type of the data
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data
     * @throws IOException if an I/O error occurs
     */
    public static <T> void displayScalarData(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T result = dataSource.readScalar();
        System.out.println(dataSet.getObjectName() + ":" + displayType(clazz, result) + READ_EQUALS + displayValue(result));

        result = dataSource.streamScalar().findFirst().orElseThrow();
        System.out.println(dataSet.getObjectName() + ":" + displayType(clazz, result) + STREAM_EQUALS + displayValue(result));
    }

    /**
     * Displays vector data from a dataset.
     * <p>
     * Reads and prints the vector data from the dataset using both direct reading and
     * streaming methods, formatting the output with type information and a comma-separated
     * list of values.
     * </p>
     *
     * @param fileChannel the seekable byte channel for reading the HDF5 file
     * @param dataSet     the dataset to read from
     * @param clazz       the class type of the data elements
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data elements
     * @throws IOException if an I/O error occurs
     */
    public static <T> void displayVectorData(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T[] resultArray = dataSource.readVector();
        System.out.println(displayType(clazz, resultArray) + READ_EQUALS + displayValue(resultArray));

        System.out.print(displayType(clazz, resultArray) + STREAM_EQUALS);
        String joined = dataSource.streamVector()
                .map(HdfDisplayUtils::displayValue)
                .collect(Collectors.joining(", "));
        System.out.print(joined);
        System.out.println("]");
    }

    /**
     * Displays vector data from a dataset.
     * <p>
     * Reads and prints the vector data from the dataset using both direct reading and
     * streaming methods, formatting the output with type information and a comma-separated
     * list of values.
     * </p>
     *
     * @param fileChannel the seekable byte channel for reading the HDF5 file
     * @param dataSet     the dataset to read from
     * @param clazz       the class type of the data elements
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data elements
     * @throws IOException if an I/O error occurs
     */
    public static <T> void displayMatrixData(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T[][] resultArray = dataSource.readMatrix();
        System.out.println(displayType(clazz, resultArray) + READ_EQUALS + displayValue(resultArray));

        System.out.print(displayType(clazz, resultArray) + STREAM_EQUALS);
        String joined = dataSource.streamMatrix()
                .map(HdfDisplayUtils::displayValue)
                .collect(Collectors.joining(", "));
        System.out.print(joined);
        System.out.println("]");
    }

    /**
     * Displays vector data from a dataset.
     * <p>
     * Reads and prints the vector data from the dataset using both direct reading and
     * streaming methods, formatting the output with type information and a comma-separated
     * list of values.
     * </p>
     *
     * @param fileChannel the seekable byte channel for reading the HDF5 file
     * @param dataSet     the dataset to read from
     * @param clazz       the class type of the data elements
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data elements
     * @throws IOException if an I/O error occurs
     */
    private static <T> void displayNDimData(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        System.out.print("read = " );
        printFlattenedArray(dataSource.readFlattened(), dataSource.getShape());
        System.out.println();

        Object resultArray = FlattenedArrayUtils.streamToNDArray(dataSource.streamFlattened(), dataSource.getShape(), clazz);
        System.out.println(displayType(clazz, resultArray) + STREAM_EQUALS + displayValue(resultArray));
    }

    // Method to print flattened array according to shape
    public static <T> void printFlattenedArray(T[] resultRead, int[] shape) {
        if (resultRead == null || shape == null || shape.length == 0) {
            System.out.print("[]");
            return;
        }
        // Start printing with an index tracker for the flattened array
        int[] index = {0}; // Mutable index to track position in resultRead
        printArray(resultRead, shape, 0, index);
    }

    private static <T> void printArray(T[] resultRead, int[] shape, int dimIndex, int[] index) {
        // Base case: at the last dimension, print a flat array
        if (dimIndex == shape.length - 1) {
            System.out.print("[");
            int size = shape[dimIndex];
            for (int i = 0; i < size; i++) {
                // Check if we have more elements in resultRead
                if (index[0] < resultRead.length) {
                    System.out.print(resultRead[index[0]]);
                    index[0]++;
                } else {
                    // Print 0 or null if out of elements
                    System.out.print("0");
                }
                if (i < size - 1) {
                    System.out.print(",");
                }
            }
            System.out.print("]");
            return;
        }

        // Recursive case: print nested arrays
        System.out.print("[");
        int currentSize = shape[dimIndex];
        for (int i = 0; i < currentSize; i++) {
            printArray(resultRead, shape, dimIndex + 1, index);
            if (i < currentSize - 1) {
                System.out.print(",");
            }
        }
        System.out.print("]");
    }

    /**
     * Formats the type information for display.
     *
     * @param declaredType the declared type of the data
     * @param actualValue  the actual value to determine the type
     * @return a string representing the type, including array component type if applicable
     */
    private static String displayType(Class<?> declaredType, Object actualValue) {
        if (actualValue == null) return declaredType.getSimpleName();
        Class<?> actualClass = actualValue.getClass();
        if (actualClass.isArray()) {
            Class<?> componentType = actualClass.getComponentType();
            return declaredType.getSimpleName() + "(" + componentType.getSimpleName() + "[])";
        }
        return declaredType.getSimpleName();
    }

    /**
     * Formats a value for display.
     *
     * @param value the value to format
     * @return a string representation of the value, handling arrays appropriately
     */
    private static String displayValue(Object value) {
        if (value == null) return "null";
        Class<?> clazz = value.getClass();
        if (!clazz.isArray()) return value.toString();

        if (clazz == int[].class) return Arrays.toString((int[]) value);
        if (clazz == float[].class) return Arrays.toString((float[]) value);
        if (clazz == double[].class) return Arrays.toString((double[]) value);
        if (clazz == long[].class) return Arrays.toString((long[]) value);
        if (clazz == short[].class) return Arrays.toString((short[]) value);
        if (clazz == byte[].class) return Arrays.toString((byte[]) value);
        if (clazz == char[].class) return Arrays.toString((char[]) value);
        if (clazz == boolean[].class) return Arrays.toString((boolean[]) value);

        return Arrays.deepToString((Object[]) value); // For Object[] or nested Object[][]
    }
}