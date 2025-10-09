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
import java.util.DoubleSummaryStatistics;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for displaying HDF5 dataset data, attributes, and summary statistics.
 * <p>
 * This class provides methods to display scalar, vector, and matrix data from HDF5 datasets
 * using a {@link TypedDataSource}. It supports various modes of display:
 * </p>
 * <ul>
 *   <li><b>Full Content:</b> Prints the entire data content of datasets.</li>
 *   <li><b>Max Value:</b> Calculates and displays the maximum value for numeric or comparable datasets.</li>
 *   <li><b>Summary Stats:</b> Computes and displays summary statistics (count, min, max, average, sum) for numeric datasets.</li>
 * </ul>
 * <p>
 * It also includes helpers for displaying file attributes and link messages.
 * </p>
 */
public class HdfDisplayUtils {

    // --- Fields & Logger ---
    private static final Logger log = LoggerFactory.getLogger(HdfDisplayUtils.class);
    public static final String UNDEFINED = "<Undefined>";
    private static final String STREAM_EQUALS = " stream = ";

    /**
     * Defines the type of information to display for each dataset.
     */
    public enum DisplayMode {
        FULL_CONTENT,
        MAX_VALUE,
        SUMMARY_STATS
    }

    // --- Common Utility Methods ---

    public static void displayLinkMessages(HdfObjectHeaderPrefix objectHeader) {
        for (HdfMessage hdfMessage : objectHeader.getHeaderMessages()) {
            if (hdfMessage instanceof LinkMessage) {
                LinkMessage linkMessage = (LinkMessage) hdfMessage;
                System.out.println("\tLinkMessage: " + linkMessage);
            }
        }
    }

    public static String undefinedArrayToString(HdfFixedPoint[] values) {
        if (values == null || values.length == 0) {
            return "Not Present";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.length; i++) {
            sb.append(values[i].isUndefined() ? UNDEFINED : values[i].toString());
            if (i != values.length - 1) {
                sb.append(", ");
            }
        }
        sb.append(']');
        return sb.toString();
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

    // --- File Processing and Public Entry Points ---

    @FunctionalInterface
    interface FileAction {
        void perform(SeekableByteChannel channel, HdfDataset dataSet, HdfFileReader reader) throws Exception;
    }

    private static void processFile(Path filePath, FileAction action) {
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            for (HdfDataset dataSet : reader.getDatasets()) {
                System.out.println("Processing: " + dataSet);
                action.perform(channel, dataSet, reader);
            }
        } catch (Exception e) {
            log.error("Exception processing file: {}", filePath, e);
        }
    }

    /** Displays the attributes for each dataset in the file. */
    public static void displayFileAttr(Path filePath) {
        processFile(filePath, (channel, dataSet, reader) -> displayAttributes(dataSet));
    }

    /** Displays the full content for each dataset in the file. */
    public static void displayFileContent(Path filePath) {
        processFile(filePath, (channel, dataSet, reader) -> displayData(channel, dataSet, reader, DisplayMode.FULL_CONTENT));
    }

    /** Displays the maximum value for each dataset in the file. */
    public static void displayFileMaxValues(Path filePath) {
        processFile(filePath, (channel, dataSet, reader) -> displayData(channel, dataSet, reader, DisplayMode.MAX_VALUE));
    }

    /** Displays summary statistics for each dataset in the file. */
    public static void displayFileSummaryStats(Path filePath) {
        processFile(filePath, (channel, dataSet, reader) -> displayData(channel, dataSet, reader, DisplayMode.SUMMARY_STATS));
    }

    // --- Internal Data Display Dispatcher ---

    public static void displayData(SeekableByteChannel channel, HdfDataset ds, HdfFileReader reader, DisplayMode mode) throws Exception {
        log.debug("Dataset path: {}", ds.getObjectPath());
        if (ds.hasData()) {
            if (mode == DisplayMode.FULL_CONTENT) {
                displayFullContentForDataset(channel, ds, reader);
            } else {
                displayAggregationForDataset(channel, ds, reader, mode);
            }
        } else if (ds.isDataset() && ds.getHardLink() != null) {
            log.info("{}: HARDLINK = {} ", ds.getObjectName(), ds.getHardLink());
        }
    }

    // --- Logic for FULL_CONTENT mode ---

    private static void displayFullContentForDataset(SeekableByteChannel channel, HdfDataset ds, HdfFileReader reader) throws Exception {
        switch (ds.getDimensionality()) {
            case 0:
                displayScalarContent(channel, ds, HdfData.class, reader);
                break;
            case 1:
                displayVectorContent(channel, ds, HdfData.class, reader);
                break;
            case 2:
                displayMatrixContent(channel, ds, HdfData.class, reader);
                break;
            default:
                displayNDimContent(channel, ds, HdfData.class, reader);
                break;
        }
    }

    public static <T> void displayScalarContent(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);
        T result = dataSource.readScalar();
        log.info("{}:{} read = {}", dataSet.getObjectName(), displayType(clazz, result), displayValue(result));

        result = dataSource.streamScalar().findFirst().orElseThrow();
        log.info("{}:{} stream = {}", dataSet.getObjectName(), displayType(clazz, result), displayValue(result));
    }

    public static <T> void displayVectorContent(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);
        T[] resultArray = dataSource.readVector();
        log.info("{} read = {}", displayType(clazz, resultArray), displayValue(resultArray));

        String joined = dataSource.streamVector().map(HdfDisplayUtils::displayValue).collect(Collectors.joining(", "));
        log.info("{} stream = [{}]", displayType(clazz, resultArray), joined);
    }

    public static <T> void displayMatrixContent(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);
        T[][] resultArray = dataSource.readMatrix();
        log.info("{} read = {}", displayType(clazz, resultArray), displayValue(resultArray));

        String joined = dataSource.streamMatrix().map(HdfDisplayUtils::displayValue).collect(Collectors.joining(", "));
        log.info("{} stream = [{}]", displayType(clazz, resultArray), joined);
    }

    public static <T> void displayNDimContent(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);
        String readResult = flattenedArrayToString(dataSource.readFlattened(), dataSource.getShape());
        log.info("read = {}", readResult);

        Object resultArray = FlattenedArrayUtils.streamToNDArray(dataSource.streamFlattened(), dataSource.getShape(), clazz);
        log.info("{}{} {}", displayType(clazz, resultArray), STREAM_EQUALS, displayValue(resultArray));
    }

    // --- Logic for Aggregation modes (MAX_VALUE, SUMMARY_STATS) ---

    private static <T extends Comparable<T>> void displayAggregationForDataset(SeekableByteChannel channel, HdfDataset ds, HdfFileReader reader, DisplayMode mode) throws Exception {
        Class<T> clazz = getClassForDatatype(ds);
        if (clazz == null) {
            log.info("{}: Cannot calculate {} for data type {}", ds.getObjectPath(), mode, ds.getDatatype().getDatatypeClass().name());
            return;
        }

        TypedDataSource<T> dataSource = new TypedDataSource<>(channel, reader, ds, clazz);
        String aggregationResult;
        String streamType;

        switch (ds.getDimensionality()) {
            case 0:
                streamType = "streamScalar";
                aggregationResult = aggregateStream(dataSource.streamScalar(), mode);
                break;
            case 1:
                streamType = "streamVector";
                aggregationResult = aggregateStream(dataSource.streamVector(), mode);
                break;
            case 2:
                streamType = "streamMatrix";
                Stream<T> matrixStream = dataSource.streamMatrix().flatMap(Arrays::stream);
                aggregationResult = aggregateStream(matrixStream, mode);
                break;
            default:
                streamType = "streamFlattened";
                aggregationResult = aggregateStream(dataSource.streamFlattened(), mode);
                break;
        }

        String output = String.format("%s %s->%s %s %s = %s",
                ds.getObjectPath(),
                ds.getDatatype().getDatatypeClass().name(),
                clazz.getSimpleName(),
                streamType,
                mode.name().toLowerCase().replace('_', ' '),
                aggregationResult);
        System.out.println(output);
    }

    private static <T extends Comparable<T>> String aggregateStream(Stream<T> stream, DisplayMode mode) {
        switch (mode) {
            case MAX_VALUE:
                Optional<T> max = stream.max(Comparator.naturalOrder());
                return max.map(Object::toString).orElse("Not Present");
            case SUMMARY_STATS:
                DoubleSummaryStatistics stats = stream.mapToDouble(HdfDisplayUtils::toDouble).summaryStatistics();
                return stats.toString();
            default:
                return "Unsupported aggregation mode";
        }
    }

    // --- Helpers for Aggregation ---

    @SuppressWarnings("unchecked")
    private static <T extends Comparable<T>> Class<T> getClassForDatatype(HdfDataset dataSet) {
        return (Class<T>) switch (dataSet.getDatatype().getDatatypeClass()) {
            case FIXED, TIME -> Long.class;
            case FLOAT -> Double.class;
            case STRING, BITFIELD, OPAQUE, COMPOUND, REFERENCE, ENUM, VLEN, ARRAY -> String.class;
        };
    }

    private static double toDouble(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }
        try {
            return Double.parseDouble(obj.toString());
        } catch (Exception ex) {
            return Double.NaN;
        }
    }

    // --- Helpers for FULL_CONTENT mode ---

    public static <T> String flattenedArrayToString(T[] flatArray, int[] shape) {
        if (flatArray == null || shape == null || shape.length == 0) return "[]";
        StringBuilder sb = new StringBuilder();
        int[] index = {0};
        arrayToString(sb, flatArray, shape, 0, index);
        return sb.toString();
    }

    private static <T> void arrayToString(StringBuilder sb, T[] flatArray, int[] shape, int dimIndex, int[] index) {
        if (dimIndex == shape.length - 1) {
            sb.append("[");
            for (int i = 0; i < shape[dimIndex]; i++) {
                if (index[0] < flatArray.length) sb.append(flatArray[index[0]++]);
                else sb.append("null");
                if (i < shape[dimIndex] - 1) sb.append(",");
            }
            sb.append("]");
            return;
        }
        sb.append("[");
        for (int i = 0; i < shape[dimIndex]; i++) {
            arrayToString(sb, flatArray, shape, dimIndex + 1, index);
            if (i < shape[dimIndex] - 1) sb.append(",");
        }
        sb.append("]");
    }

    private static String displayType(Class<?> declaredType, Object actualValue) {
        if (actualValue == null) return declaredType.getSimpleName();
        Class<?> actualClass = actualValue.getClass();
        if (actualClass.isArray()) {
            Class<?> componentType = actualClass.getComponentType();
            return declaredType.getSimpleName() + "(" + componentType.getSimpleName() + "[])";
        }
        return declaredType.getSimpleName();
    }

    public static String displayValue(Object value) {
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
        return Arrays.deepToString((Object[]) value);
    }
}