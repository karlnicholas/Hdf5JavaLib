package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.message.*;
import com.github.karlnicholas.hdf5javalib.utils.HdfDataSource;
import com.github.karlnicholas.hdf5javalib.utils.HdfSpliterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.*;

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
//                new HdfConstruction().buildHfd();
                tryHdfFileBuilder();
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

        builder.superblock(4, 16, 0, 100320);

        // Define a root group
        builder.rootGroup(96);

        builder.objectHeader();

            // Define the heap data size
            int dataSegmentSize = 88;

            // Initialize the heapData array
            byte[] heapData = new byte[dataSegmentSize];
            Arrays.fill(heapData, (byte) 0); // Set all bytes to 0

            // Set bytes 8-16 to the required values
            heapData[8] = 68;  // 'D'
            heapData[9] = 101; // 'e'
            heapData[10] = 109; // 'm'
            heapData[11] = 97; // 'a'
            heapData[12] = 110; // 'n'
            heapData[13] = 100; // 'd'
            heapData[14] = 0;  // null terminator
            heapData[15] = 0;  // additional null byte

        builder.localHeap(dataSegmentSize, 16, 712, heapData);

// Define a dataset with correct CompoundDataType members
        // DataTypeMessage with CompoundDataType
        List<CompoundDataType.Member> members = List.of(
                new CompoundDataType.Member("Id", 0, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)8, false, false, false, false, 0, 64)),
                new CompoundDataType.Member("origCountry", 8, 0, 0, new int[4],
                        new CompoundDataType.StringMember((short)2, 0, "Null Terminate", 0, "ASCII")),
                new CompoundDataType.Member("origSlic", 10, 0, 0, new int[4],
                        new CompoundDataType.StringMember((short)5, 0, "Null Terminate", 0, "ASCII")),
                new CompoundDataType.Member("origSort", 15, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)1, false, false, false, false, 0, 8)),
                new CompoundDataType.Member("destCountry", 16, 0, 0, new int[4],
                        new CompoundDataType.StringMember((short)2, 0, "Null Terminate", 0, "ASCII")),
                new CompoundDataType.Member("destSlic", 18, 0, 0, new int[4],
                        new CompoundDataType.StringMember((short)5, 0, "Null Terminate", 0, "ASCII")),
                new CompoundDataType.Member("destIbi", 23, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)1, false, false, false, false, 0, 8)),
                new CompoundDataType.Member("destPostalCode", 40, 0, 0, new int[4],
                        new CompoundDataType.StringMember((short)9, 0, "Null Terminate", 0, "ASCII")),
                new CompoundDataType.Member("shipper", 24, 0, 0, new int[4],
                        new CompoundDataType.StringMember((short)10, 0, "Null Terminate", 0, "ASCII")),
                new CompoundDataType.Member("service", 49, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)1, false, false, false, false, 0, 8)),
                new CompoundDataType.Member("packageType", 50, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)1, false, false, false, false, 0, 8)),
                new CompoundDataType.Member("accessorials", 51, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)1, false, false, false, false, 0, 8)),
                new CompoundDataType.Member("pieces", 52, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)2, false, false, false, false, 0, 16)),
                new CompoundDataType.Member("weight", 34, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)2, false, false, false, false, 0, 16)),
                new CompoundDataType.Member("cube", 36, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)4, false, false, false, false, 0, 32)),
                new CompoundDataType.Member("committedTnt", 54, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)1, false, false, false, false, 0, 8)),
                new CompoundDataType.Member("committedDate", 55, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((short)1, false, false, false, false, 0, 8))
        );

        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(new ContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
        headerMessages.add(new NullMessage());

        // Define Compound DataType correctly
        CompoundDataType compoundType = new CompoundDataType(members.size(), 56, members);
        DataTypeMessage dataTypeMessage = new DataTypeMessage(1, 6, BitSet.valueOf(new long[]{0b10001}), HdfFixedPoint.of(56), compoundType);
//        dataTypeMessage.setDataType(compoundType);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        headerMessages.add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        int[] dimensionSizes=new int[] {0, 0, 0, 0};
        HdfFixedPoint[] hdfDimensionSizes = Arrays.stream(dimensionSizes).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(1, 1, HdfFixedPoint.of(2208), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined((short)8));
        headerMessages.add(dataLayoutMessage);

        // add ObjectModification Time message
        headerMessages.add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

        // Add DataSpaceMessage (Handles dataset dimensionality)
        HdfFixedPoint[] hdfDimensions = Arrays.stream(new long[]{1750}).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataSpaceMessage dataSpaceMessage = new DataSpaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
        headerMessages.add(dataSpaceMessage);

        String attributeName = "GIT root revision";
        String attributeValue = "Revision: , URL: ";
        headerMessages.add(new AttributeMessage(1, attributeName.length(), 8, 8, new HdfString(attributeName, false), new HdfString(attributeValue, false)));

        // new long[]{1750}, new long[]{98000}
        builder.addDataset(headerMessages);

        // Define a B-Tree for group indexing
        builder.addBTree(1880, "Demand");

        // Define a Symbol Table Node
        builder.addSymbolTableNode(800);

        // Write to an HDF5 file
        builder.writeToFile("output.hdf5");

    }
}
