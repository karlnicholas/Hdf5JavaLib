package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.utils.HdfDataSource;
import com.github.karlnicholas.hdf5javalib.utils.HdfSpliterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
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
                new HdfConstruction().buildHfd();
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

    public static void tryHdfFileBuilder() {
        HdfFileBuilder builder = new HdfFileBuilder();

// Define a root group
        builder.addGroup("root", 96);

        int[] dimensionSizes=new int[] {0, 0, 0, 0};
// Define a dataset with correct CompoundDataType members
        List<CompoundDataType.Member> members = Arrays.asList(
                new CompoundDataType.Member("shipmentId", 0, 0, 0, dimensionSizes, new CompoundDataType.FixedPointMember(8, false, false, false, false, 0, 64)),
                new CompoundDataType.Member("origCountry", 8, 0, 0, dimensionSizes,new CompoundDataType.StringMember(2, 0, "Null Terminate", 0, "ASCII"))
        );
        builder.addDataset("shipmentData", 800, members, new long[]{12345, 67890});

// Define a B-Tree for group indexing
        builder.addBTree(1880, "Demand");

// Define a Symbol Table Node
        builder.addSymbolTableNode(800);

// Write to an HDF5 file
        builder.writeToFile("output.hdf5");

    }
}
