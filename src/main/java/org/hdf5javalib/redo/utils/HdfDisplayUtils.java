package org.hdf5javalib.redo.utils;

import org.hdf5javalib.redo.HdfFileReader;
import org.hdf5javalib.redo.dataclass.HdfData;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.datasource.TypedDataSource;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfDataSet;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
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

    /**
     * Creates a version attribute for a dataset.
     * <p>
     * Adds a "GIT root revision" attribute to the specified dataset with a predefined
     * value containing revision and URL information.
     * </p>
     *
     * @param hdfDataFile the HDF5 file context
     * @param dataset     the dataset to which the attribute is added
     */
    public static void writeVersionAttribute(HdfDataFile hdfDataFile, HdfDataSet dataset) {
        String ATTRIBUTE_NAME = "GIT root revision";
        String ATTRIBUTE_VALUE = "Revision: , URL: ";
        dataset.createAttribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE, hdfDataFile);
    }

    public static void displayData(SeekableByteChannel channel, HdfDataSet ds, HdfFileReader reader) throws Exception {
        System.out.println(ds.getDatasetName());
        if (ds.hasDataspaceMessage()) {
            switch (ds.getDimensionality()) {
                case 0:
                    if (HdfFixedPoint.compareToZero(ds.getdimensionSizes().orElseThrow()[0]) != 0) {
                        displayScalarData(channel, ds, HdfData.class, reader);
                    }
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
    public static <T> void displayScalarData(SeekableByteChannel fileChannel, HdfDataSet dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T result = dataSource.readScalar();
        System.out.println(dataSet.getDatasetName() + ":" + displayType(clazz, result) + " read   = " + displayValue(result));

        result = dataSource.streamScalar().findFirst().orElseThrow();
        System.out.println(dataSet.getDatasetName() + ":" + displayType(clazz, result) + " stream = " + displayValue(result));
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
    public static <T> void displayVectorData(SeekableByteChannel fileChannel, HdfDataSet dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T[] resultArray = dataSource.readVector();
        System.out.println(displayType(clazz, resultArray) + " read   = " + displayValue(resultArray));

        System.out.print(displayType(clazz, resultArray) + " stream = [");
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
    public static <T> void displayMatrixData(SeekableByteChannel fileChannel, HdfDataSet dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T[][] resultArray = dataSource.readMatrix();
        System.out.println(displayType(clazz, resultArray) + " read   = " + displayValue(resultArray));

        System.out.print(displayType(clazz, resultArray) + " stream = [");
        String joined = dataSource.streamMatrix()
                .map(HdfDisplayUtils::displayValue)
                .collect(Collectors.joining(", "));
        System.out.print(joined);
        System.out.println("]");
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