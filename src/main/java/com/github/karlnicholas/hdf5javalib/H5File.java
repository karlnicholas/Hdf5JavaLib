package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.message.*;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

@Getter
public class H5File {
    private final String fileName;
    private final StandardOpenOption[] openOptions;
    private final HdfFileBuilder builder;


    public H5File(String fileName, StandardOpenOption[] openOptions) {
        this.fileName = fileName;
        this.openOptions = openOptions;
        builder = new HdfFileBuilder();
        builder.superblock(4, 16, 0, 100320);
        // Define a root group
        builder.rootGroup(96);
        builder.objectHeader();

    }

    public <T> HdfDataSet<T> createDataSet(String datasetName, CompoundDataType compoundType, HdfFixedPoint[] hdfDimensions) {
        // Define the heap data size
        int dataSegmentSize = 88;

        // Initialize the heapData array
        byte[] heapData = new byte[dataSegmentSize];
        Arrays.fill(heapData, (byte) 0); // Set all bytes to 0
        System.arraycopy(datasetName.getBytes(StandardCharsets.US_ASCII), 0, heapData, 8, datasetName.length());

        builder.localHeap(dataSegmentSize, 16, 712, heapData);


        return new HdfDataSet<>(this, datasetName, compoundType, hdfDimensions);
    }

    public HdfFileBuilder buildMetaData(FileChannel fileChannel, String datasetName, CompoundDataType compoundDataType) throws IOException {

        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(new ContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
        headerMessages.add(new NullMessage());

        DataTypeMessage dataTypeMessage = new DataTypeMessage(1, 6, BitSet.valueOf(new byte[]{0b10001}), new HdfFixedPoint(false, new byte[]{(byte)56}, (short)4), compoundDataType);
//        dataTypeMessage.setDataType(compoundType);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        headerMessages.add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] hdfDimensionSizes = { HdfFixedPoint.of(98000)};
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1, HdfFixedPoint.of(2208), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined((short)8));
        headerMessages.add(dataLayoutMessage);

        // add ObjectModification Time message
        headerMessages.add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

        // Add DataSpaceMessage (Handles dataset dimensionality)
        HdfFixedPoint[] hdfDimensions = Arrays.stream(new long[]{1750}).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataSpaceMessage dataSpaceMessage = new DataSpaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
        headerMessages.add(dataSpaceMessage);

//        String attributeName = "GIT root revision";
//        String attributeValue = "Revision: , URL: ";
//        DataTypeMessage dt = new DataTypeMessage(1, 3, BitSet.valueOf(new byte[0]), HdfFixedPoint.of(attributeName.length()+1), new HdfString(attributeName, false));
//        DataSpaceMessage ds = new DataSpaceMessage(1, 1, 1, new HdfFixedPoint[] {HdfFixedPoint.of(1)}, null, false);
//        headerMessages.add(new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), new HdfString(attributeValue, false)));

        // new long[]{1750}, new long[]{98000}
        builder.addDataset(headerMessages);

        // Define a B-Tree for group indexing
        builder.addBTree(1880, "Demand");

        // Define a Symbol Table Node
        builder.addSymbolTableNode(800);

        // Write to an HDF5 file
        builder.writeToFile(fileChannel);

        return builder;

    }

    public void write(Supplier<ByteBuffer> bufferSupplier, String datasetName, CompoundDataType compoundDataType, HdfFixedPoint[] hdfDimensions) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            HdfFileBuilder hdfFileBuilder = buildMetaData(fileChannel, datasetName, compoundDataType);
            long dataAddress = hdfFileBuilder.dataAddress();
            fileChannel.position(dataAddress);
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                fileChannel.write(buffer);
            }
        }
    }
}
