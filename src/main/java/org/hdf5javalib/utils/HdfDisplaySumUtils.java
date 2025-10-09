package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.hdffile.dataobjects.messages.AttributeMessage;
import org.hdf5javalib.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.hdffile.dataobjects.messages.LinkMessage;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;

/**
 * Utility class for displaying HDF5 dataset data and managing attributes.
 * <p>
 * The {@code HdfDisplayUtils} class provides methods to display scalar and vector data
 * from HDF5 datasets using a {@link TypedDataSource}, as well as to create version attributes
 * for datasets. It supports various data types and formats the output for easy inspection,
 * handling both primitive and array types.
 * </p>
 */
public class HdfDisplaySumUtils {
    private static final Logger log = LoggerFactory.getLogger(HdfDisplaySumUtils.class);
    public static final String UNDEFINED = "<Undefined>";
    private static final String STREAM_EQUALS = " stream = ";

    public static void displayLinkMessages(HdfObjectHeaderPrefix objectHeader) {
        for( HdfMessage hdfMessage: objectHeader.getHeaderMessages()) {
            if ( hdfMessage instanceof LinkMessage ) {
                LinkMessage linkMessage = (LinkMessage) hdfMessage;
                System.out.println("\tLinkMessage: " + linkMessage.toString());
            }

        }
    }

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
                System.out.println("{} " + dataSet);
//                log.info("{} ", dataSet);
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
        processFile(filePath, HdfDisplaySumUtils::displayData);
    }

    public static void displayAttributes(HdfDataset dataSet) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        for (AttributeMessage message : dataSet.getAttributeMessages()) {
            HdfDataHolder dataHolder = message.getHdfDataHolder();
            if (dataHolder.getDimensionality() == 1) {
                HdfData[] data = dataHolder.getAll(HdfData[].class);
                log.info("Data = {}", Arrays.toString(data));
            } else if (dataHolder.getDimensionality() == 2) {
                HdfData[][] data = dataHolder.getAll(HdfData[][].class);
                for (HdfData[] row : data) {
                    log.info("Row = {}", Arrays.toString(row));
                }
            }
        }
    }

//    public static String getDataObjectFullName(HdfDataObject hdfDataObject) {
//        List<String> parents = new ArrayList<>();
//        HdfDataObject currentNode = hdfDataObject;
//        while(currentNode.getParent() != null) {
//            parents.add(currentNode.getObjectName());
//            currentNode = currentNode.getParent().getDataObject();
//        }
//        Collections.reverse(parents);
//        String objectPathString = '/' + currentNode.getObjectName() + String.join("/", parents);
//        return objectPathString;
//    }

    public static void displayData(SeekableByteChannel channel, HdfDataset ds, HdfFileReader reader) throws Exception {
        log.debug("Dataset path: {}", ds.getObjectPath());
        if (ds.hasData()) {
            switch (ds.getDimensionality()) {
                case 0:
                    displayScalarData(channel, ds, reader);
                    break;
                case 1:
                    displayVectorData(channel, ds, reader);
                    break;
                case 2:
                    displayMatrixData(channel, ds, reader);
                    break;
                default:
                    displayNDimData(channel, ds, reader);
                    break;

            }
        } else if (ds.isDataset() && ds.getHardLink() != null) {
            log.info("{}: HARDLINK = {} ", ds.getObjectName(), ds.getHardLink());
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
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data
     * @throws IOException if an I/O error occurs
     */
    public static <T extends Comparable<T>> void displayScalarData(SeekableByteChannel fileChannel, HdfDataset dataSet, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<T> clazz = getClassForDatatype(dataSet);
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

//        Optional<String> max = dataSource.streamScalar().map(h->h.toString()).max(Comparator.naturalOrder());
        Optional<T> max = dataSource.streamScalar().max(Comparator.naturalOrder());
        System.out.println(dataSet.getObjectPath() + " " + dataSet.getDatatype().getDatatypeClass().name() + "->" + clazz.getSimpleName() + " streamScalar max = " + (max.isPresent() ? max.get().toString() : "NaN"));
//        long count = dataSource.parallelStreamScalar().count();
//        System.out.println(dataSet.getObjectPath() + " stream count = " + String.format("%,d", count) + ":" + dataSet.getDatatype().toString());
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
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data elements
     * @throws IOException if an I/O error occurs
     */
    public static <T extends Comparable<T>>  void displayVectorData(SeekableByteChannel fileChannel, HdfDataset dataSet, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<T> clazz = getClassForDatatype(dataSet);
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

//        T[] resultArray = dataSource.readVector();
//        log.info("{} read = {}", displayType(clazz, resultArray), displayValue(resultArray));

//        Optional<String> max = dataSource.streamVector().map(h->h.toString()).max(Comparator.naturalOrder());
        Optional<T> max = dataSource.streamVector().max(Comparator.naturalOrder());
        System.out.println(dataSet.getObjectPath() + " " + dataSet.getDatatype().getDatatypeClass().name() + "->" + clazz.getSimpleName() + " streamVector max = " + (max.isPresent() ? max.get().toString() : "NaN"));

//        long count = dataSource.parallelStreamVector().count();
//        System.out.println(dataSet.getObjectPath() + " stream count = " + String.format("%,d", count) + ":" + dataSet.getDatatype().toString());
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
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data elements
     * @throws IOException if an I/O error occurs
     */
    public static <T extends Comparable<T>> void displayMatrixData(SeekableByteChannel fileChannel, HdfDataset dataSet, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<T> clazz = getClassForDatatype(dataSet);
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        Optional<T> max = dataSource.streamMatrix().flatMap(h->Arrays.stream(h)).max(Comparator.naturalOrder());
        System.out.println(dataSet.getObjectPath() + " " + dataSet.getDatatype().getDatatypeClass().name() + "->" + clazz.getSimpleName() + " streamMatrix max = " + (max.isPresent() ? max.get().toString() : "NaN"));

//        long count = dataSource.parallelStreamMatrix().count();
//        System.out.println(dataSet.getObjectPath() + " stream count = " + String.format("%,d", count) + ":" + dataSet.getDatatype().toString());
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
     * @param hdfDataFile the HDF5 file context
     * @param <T>         the type of the data elements
     * @throws IOException if an I/O error occurs
     */
    private static <T extends Comparable<T>> void displayNDimData(SeekableByteChannel fileChannel, HdfDataset dataSet, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<T> clazz = getClassForDatatype(dataSet);
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);
//        String readResult = flattenedArrayToString(dataSource.readFlattened(), dataSource.getShape());
//        log.info("read = {}", readResult);

//        Optional<String> max = dataSource.streamFlattened().map(h->h.toString()).max(Comparator.naturalOrder());
        Optional<T> max = dataSource.streamFlattened().max(Comparator.naturalOrder());
        System.out.println(dataSet.getObjectPath() + " " + dataSet.getDatatype().getDatatypeClass().name() + "->" + clazz.getSimpleName() + " streamFlattened max = " + (max.isPresent() ? max.get().toString() : "NaN"));

        //        long count = dataSource.parallelStreamFlattened().count();
//        System.out.println(dataSet.getObjectPath() + " stream count = " + String.format("%,d", count) + ":" + dataSet.getDatatype().toString());
    }

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> Class<T> getClassForDatatype(HdfDataset dataSet) {
        return (Class<T>) switch (dataSet.getDatatype().getDatatypeClass()) {
            case FIXED, TIME -> Long.class;
            case FLOAT -> Double.class;
            case STRING, BITFIELD, OPAQUE, COMPOUND, REFERENCE, ENUM, VLEN, ARRAY -> String.class;
        };

    }
}