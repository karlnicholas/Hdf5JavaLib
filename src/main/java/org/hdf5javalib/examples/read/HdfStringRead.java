package org.hdf5javalib.examples.read;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;

/**
 * Hello world!
 *
 */
public class HdfStringRead {
    public static void main(String[] args) {
        new HdfStringRead().run();
    }

    private void run() {
        try {
            String filePath = Objects.requireNonNull(HdfStringRead.class.getResource("/ascii_dataset.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("strings") ) {
                    HdfDisplayUtils.displayVectorData(channel, dataSet, String.class, reader);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            String filePath = Objects.requireNonNull(HdfStringRead.class.getResource("/utf8_dataset.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                HdfFileReader reader = new HdfFileReader(channel).readFile();
                try ( HdfDataSet dataSet = reader.getRootGroup().findDataset("strings") ) {
                    HdfDisplayUtils.displayVectorData(channel, dataSet, String.class, reader);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
