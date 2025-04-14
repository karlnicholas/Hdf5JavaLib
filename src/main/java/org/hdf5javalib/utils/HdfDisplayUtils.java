package org.hdf5javalib.utils;

import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.Collectors;

public class HdfDisplayUtils {

//    private void writeVersionAttribute(HdfDataSet dataset) {
//        String ATTRIBUTE_NAME = "GIT root revision";
//        String ATTRIBUTE_VALUE = "Revision: , URL: ";
//        BitSet classBitField = StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII);
//        // value
//        StringDatatype attributeType = new StringDatatype(StringDatatype.createClassAndVersion(), classBitField, (short) ATTRIBUTE_VALUE.length());
//        // data type, String, DATASET_NAME.length
//        short dataTypeMessageSize = 8;
//        dataTypeMessageSize += attributeType.getSizeMessageData();
//        // to 8 byte boundary
//        dataTypeMessageSize += ((dataTypeMessageSize + 7) & ~7);
//        DatatypeMessage dt = new DatatypeMessage(attributeType, (byte)1, dataTypeMessageSize);
//        HdfFixedPoint[] hdfDimensions = {};
//        // scalar, 1 string
//        short dataspaceMessageSize = 8;
//        DataspaceMessage ds = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false),
//                hdfDimensions, hdfDimensions, false, (byte)0, dataspaceMessageSize);
//        HdfString hdfString = new HdfString(ATTRIBUTE_VALUE.getBytes(), attributeType);
//        dataset.createAttribute(ATTRIBUTE_NAME, dt, ds, hdfString);
//    }
//

    public static void writeVersionAttribute(HdfDataFile hdfDataFile, HdfDataSet dataset) {
        String ATTRIBUTE_NAME = "GIT root revision";
        String ATTRIBUTE_VALUE = "Revision: , URL: ";
        dataset.createAttribute(ATTRIBUTE_NAME, ATTRIBUTE_VALUE, hdfDataFile);
    }

    public static <T> void displayScalarData(SeekableByteChannel fileChannel, HdfDataSet dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T result = dataSource.readScalar();
        System.out.println(displayType(clazz, result) + " read   = " + displayValue(result));

        result = dataSource.streamScalar().findFirst().orElseThrow();
        System.out.println(displayType(clazz, result) + " stream = " + displayValue(result));
    }

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

    private static String displayType(Class<?> declaredType, Object actualValue) {
        if (actualValue == null) return declaredType.getSimpleName();
        Class<?> actualClass = actualValue.getClass();
        if (actualClass.isArray()) {
            Class<?> componentType = actualClass.getComponentType();
            return declaredType.getSimpleName() + "(" + componentType.getSimpleName() + "[])";
        }
        return declaredType.getSimpleName();
    }

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
