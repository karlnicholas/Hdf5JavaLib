package com.github.karlnicholas.hdf5javalib.file;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Getter
public class HdfFile {
    private final String fileName;
    private final StandardOpenOption[] openOptions;
    private final HdFileHeaderBuilder builder;
    private final AtomicLong messageCount;

    public HdfFile(String fileName, StandardOpenOption[] openOptions) {
        this.fileName = fileName;
        this.openOptions = openOptions;
        builder = new HdFileHeaderBuilder();

        builder.superblock(4, 16, 0, 100320);
        // Define a root group
        builder.rootGroup(96);
        builder.objectHeader();

        // Define the heap data size, why 88 I don't know.
        int dataSegmentSize = 88;
        // Initialize the heapData array
        byte[] heapData = new byte[dataSegmentSize];
        Arrays.fill(heapData, (byte) 0); // Set all bytes to 0

        builder.localHeap(dataSegmentSize, 16, 712, heapData);

        // Define a B-Tree for group indexing
        builder.addBTree();
        messageCount = new AtomicLong();

    }

    public <T> HdfDataSet<T> createDataSet(String datasetName, CompoundDataType compoundType, HdfFixedPoint[] hdfDimensions) {

        builder.addBTree(1880, datasetName);

        // Define a Symbol Table Node
        builder.addSymbolTableNode(800);

        return new HdfDataSet<>(this, datasetName, compoundType, hdfDimensions, HdfFixedPoint.of(2208));
    }

    public void write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet<?> hdfDataSet) throws IOException {
        messageCount.set(0);
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            long dataAddress = hdfDataSet.getDatasetAddress().getBigIntegerValue().longValue();
            fileChannel.position(dataAddress);
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                messageCount.incrementAndGet();
                fileChannel.write(buffer);
            }
        }
    }

    public <T> void closeDataset(HdfDataSet<T> hdfDataSet) throws IOException {
        // Initialize the localHeapContents heapData array
        System.arraycopy(hdfDataSet.getDatasetName().getBytes(StandardCharsets.US_ASCII), 0, builder.getLocalHeapContents().getHeapData(), 8, hdfDataSet.getDatasetName().length());
        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(new ObjectHeaderContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
        headerMessages.add(new NilMessage());

        DatatypeMessage dataTypeMessage = new DatatypeMessage(1, 6, BitSet.valueOf(new byte[]{0b10001}), new HdfFixedPoint(false, new byte[]{(byte)56}, (short)4), hdfDataSet.getCompoundDataType());
//        dataTypeMessage.setDataType(compoundType);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        headerMessages.add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] hdfDimensionSizes = { HdfFixedPoint.of(messageCount.get())};
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1, HdfFixedPoint.of(2208), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined((short)8));
        headerMessages.add(dataLayoutMessage);

        // add ObjectModification Time message
        headerMessages.add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

        // Add DataspaceMessage (Handles dataset dimensionality)
//        HdfFixedPoint[] hdfDimensions = Arrays.stream(new long[]{1750}).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
        DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDataSet.getHdfDimensions(), hdfDataSet.getHdfDimensions(), true);
        headerMessages.add(dataSpaceMessage);

//        headerMessages.addAll(hdfDataSet.getAttributes());

        // new long[]{1750}, new long[]{98000}
        builder.addDataset(headerMessages);
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), StandardOpenOption.WRITE)) {
            fileChannel.position(0);
            builder.writeToFile(fileChannel);
        }
    }
}
