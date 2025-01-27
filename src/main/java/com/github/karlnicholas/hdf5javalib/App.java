package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.utils.HdfDataSource;
import com.github.karlnicholas.hdf5javalib.utils.HdfSpliterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Spliterator;

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
//                printData(channel, reader.getCompoundDataType(), reader.getDataAddress(), reader.getDimension());
//                trySpliterator(channel, reader);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void trySpliterator(FileChannel fileChannel, HdfReader reader) {

        HdfDataSource<VolumeData> hdfDataSource = new HdfDataSource(reader.getCompoundDataType(), VolumeData.class);

        Spliterator<VolumeData> spliterator = new HdfSpliterator(fileChannel, reader.getDataAddress(), reader.getCompoundDataType().getSize(), reader.getDimension(), hdfDataSource);

        spliterator.forEachRemaining(buffer -> {
            // Process each ByteBuffer (record) here
            System.out.println("Record: " + buffer);
        });

    }
}
