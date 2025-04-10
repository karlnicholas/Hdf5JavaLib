package org.hdf5javalib.utils;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class HdfTestUtils {
    // Get a TypedDataSource for a dataset in a file
    public static <T> TypedDataSource<T> getDataSource(String fileName, String datasetName, Class<T> type) throws IOException {
        FileInputStream fis = new FileInputStream(getResourcePath(fileName));
        FileChannel channel = fis.getChannel();
        HdfFileReader reader = new HdfFileReader(channel).readFile();
        HdfDataSet dataSet = reader.findDataset(datasetName, channel, reader.getRootGroup());
        TypedDataSource<T> dataSource = new TypedDataSource<>(dataSet, channel, reader, type);
        fis.close(); // Close FIS since TypedDataSource handles channel and dataset
        return dataSource;
    }

    // Helper to get resource path
    private static String getResourcePath(String fileName) {
        return HdfDisplayUtils.class.getClassLoader().getResource(fileName).getPath();
    }
}