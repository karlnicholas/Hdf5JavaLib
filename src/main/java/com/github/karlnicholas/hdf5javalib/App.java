package com.github.karlnicholas.hdf5javalib;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.printData;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        try {
            HdfReader reader = new HdfReader();
            String filePath = App.class.getResource("/ExportedNodeShips.h5").getFile();
            try(FileInputStream fis = new FileInputStream(new File(filePath))) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                printData(channel, reader.getCompoundDataType(), reader.getDataAddress(), reader.getDimension());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
