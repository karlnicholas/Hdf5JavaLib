package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.data.FixedPointDataSource;
import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.CompoundDatatype;
import com.github.karlnicholas.hdf5javalib.datatype.FixedPointDatatype;
import com.github.karlnicholas.hdf5javalib.datatype.HdfCompoundDatatypeMember;
import com.github.karlnicholas.hdf5javalib.datatype.StringDatatype;
import com.github.karlnicholas.hdf5javalib.file.HdfAllocator;
import com.github.karlnicholas.hdf5javalib.file.HdfDataSet;
import com.github.karlnicholas.hdf5javalib.file.HdfFile;
import com.github.karlnicholas.hdf5javalib.file.HdfReader;
import com.github.karlnicholas.hdf5javalib.message.DataspaceMessage;
import com.github.karlnicholas.hdf5javalib.utils.HdfFixedPointDatatypeSpliterator;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        new App().run();
    }
    private void run() {
        try {
            HdfReader reader = new HdfReader();
            String filePath = Objects.requireNonNull(App.class.getResource("/randomints.h5")).getFile();
//            String filePath = App.class.getResource("/ExportedNodeShips.h5").getFile();
//            String filePath = App.class.getResource("/ForecastedVolume_2025-01-10.h5").getFile();
//            String filePath = App.class.getResource("/singleint.h5").getFile();
            try(FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
//                printData(channel, reader.getCompoundDataType(), reader.getDataAddress(), reader.getDimension());
//                trySpliterator(channel, reader);
//                new HdfConstruction().buildHfd();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        tryHdfApiCompound();
        tryHdfApiInts();
    }

    public void tryHdfApiInts() {
        final String FILE_NAME = "randomints.h5";
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "temperature";
        final int NUM_RECORDS = 100;

        try {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(FILE_NAME, FILE_OPTIONS);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(NUM_RECORDS)};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, false);
//            hsize_t dim[1] = { NUM_RECORDS };
//            DataSpace space(1, dim);

            FixedPointDatatype fixedPointDatatype = new FixedPointDatatype((byte) 1, 8, false, false, false, true, (short)0, (short)64, (short)8, BitSet.valueOf(new byte[]{0b01000}));

            // Create dataset
//            DataSet dataset = file.createDataSet(DATASET_NAME, compoundType, space);
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, fixedPointDatatype, dataSpaceMessage);

            AtomicInteger countHolder = new AtomicInteger(0);
            FixedPointDataSource<TemperatureData> temperatureDataHdfDataSource = new FixedPointDataSource<>(fixedPointDatatype, "temperature", TemperatureData.class);
            ByteBuffer temperatureBuffer = ByteBuffer.allocate(fixedPointDatatype.getSize());
            // Write to dataset
            dataset.write(() -> {
                int count = countHolder.getAndIncrement();
                if (count >= NUM_RECORDS) return  ByteBuffer.allocate(0);
                TemperatureData instance = TemperatureData.builder()
                        .temperature(BigInteger.valueOf((long) (Math.random() *40.0 + 10.0)))
                        .build();
                temperatureBuffer.clear();
                temperatureDataHdfDataSource.writeToBuffer(instance, temperatureBuffer);
                temperatureBuffer.flip();
                return temperatureBuffer;
            });
            dataset.close();
            file.close();

            // auto close

            System.out.println("HDF5 file created and written successfully!");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void tryHdfApiCompound() {
        final String FILE_NAME = "testone.h5";
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "Demand";
        final String ATTRIBUTE_NAME = "GIT root revision";
        final int NUM_RECORDS = 1750;

        try {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(FILE_NAME, FILE_OPTIONS);

            // DatatypeMessage with CompoundDatatype
            List<HdfCompoundDatatypeMember> shipment = List.of(
                    new HdfCompoundDatatypeMember("shipmentId", 0, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)8, false, false, false, false, (short)0, (short)64, computeFixedMessageDataSize("shipmentId"), new BitSet())),
                    new HdfCompoundDatatypeMember("origCountry", 8, 0, 0, new int[4],
                            new StringDatatype((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origCountry"))),
                    new HdfCompoundDatatypeMember("origSlic", 10, 0, 0, new int[4],
                            new StringDatatype((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origSlic"))),
                    new HdfCompoundDatatypeMember("origSort", 15, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("origSort"), new BitSet())),
                    new HdfCompoundDatatypeMember("destCountry", 16, 0, 0, new int[4],
                            new StringDatatype((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destCountry"))),
                    new HdfCompoundDatatypeMember("destSlic", 18, 0, 0, new int[4],
                            new StringDatatype((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destSlic"))),
                    new HdfCompoundDatatypeMember("destIbi", 23, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("destIbi"), new BitSet())),
                    new HdfCompoundDatatypeMember("destPostalCode", 40, 0, 0, new int[4],
                            new StringDatatype((byte) 1, (short)9, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destPostalCode"))),
                    new HdfCompoundDatatypeMember("shipper", 24, 0, 0, new int[4],
                            new StringDatatype((byte) 1, (short)10, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("shipper"))),
                    new HdfCompoundDatatypeMember("service", 49, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("service"), new BitSet())),
                    new HdfCompoundDatatypeMember("packageType", 50, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("packageType"), new BitSet())),
                    new HdfCompoundDatatypeMember("accessorials", 51, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("accessorials"), new BitSet())),
                    new HdfCompoundDatatypeMember("pieces", 52, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("pieces"), new BitSet())),
                    new HdfCompoundDatatypeMember("weight", 34, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("weight"), new BitSet())),
                    new HdfCompoundDatatypeMember("cube", 36, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)4, false, false, false, false, (short)0, (short)32, computeFixedMessageDataSize("cube"), new BitSet())),
                    new HdfCompoundDatatypeMember("committedTnt", 54, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedTnt"), new BitSet())),
                    new HdfCompoundDatatypeMember("committedDate", 55, 0, 0, new int[4],
                            new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedDate"), new BitSet()))
            );
            short compoundSize = (short) shipment.stream().mapToInt(c->c.getType().getSizeMessageData()).sum();
            // Define Compound DataType correctly
            CompoundDatatype compoundType = new CompoundDatatype(shipment.size(), compoundSize, shipment);
//            DatatypeMessage dataTypeMessage = new DatatypeMessage((byte) 1, (byte) 6, BitSet.valueOf(new byte[]{0b10001}), 56, compoundType);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(NUM_RECORDS)};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
//            hsize_t dim[1] = { NUM_RECORDS };
//            DataSpace space(1, dim);

            // Create dataset
//            DataSet dataset = file.createDataSet(DATASET_NAME, compoundType, space);
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, compoundType, dataSpaceMessage);


            // ADD ATTRIBUTE: "GIT root revision"
            String attributeValue = "Revision: , URL: ";
            HdfFixedPoint attr_type = HdfFixedPoint.of(ATTRIBUTE_NAME.length()+1);
            HdfFixedPoint[] attr_space = new HdfFixedPoint[] {HdfFixedPoint.of(1)};
//            AttributeMessage attributeMessage = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
//            attributeMessage.write(attr_type, attributeValue);

//            AtomicInteger countHolder = new AtomicInteger(0);
//            CompoundDataSource<VolumeData> volumeDataHdfDataSource = new CompoundDataSource<>(compoundType, VolumeData.class);
//            ByteBuffer volumeBuffer = ByteBuffer.allocate(compoundType.getSize());
//            // Write to dataset
//            dataset.write(() -> {
//                int count = countHolder.getAndIncrement();
//                if (count >= NUM_RECORDS) return  ByteBuffer.allocate(0);
//                VolumeData instance = VolumeData.builder()
//                        .shipmentId(BigInteger.valueOf(count + 1000))
//                        .origCountry("US")
//                        .origSlic("1234")
//                        .origSort(BigInteger.valueOf(4))
//                        .destCountry("US")
//                        .destSlic("4321")
//                        .destIbi(BigInteger.ZERO)
//                        .destPostalCode("94211")
//                        .shipper("DexEf")
//                        .packageType(BigInteger.ONE)
//                        .accessorials(BigInteger.ZERO)
//                        .pieces(BigInteger.valueOf(10))
//                        .pieces(BigInteger.valueOf(50))
//                        .cube(BigInteger.valueOf(1200))
//                        .committedTnt(BigInteger.ZERO)
//                        .committedDate(BigInteger.ZERO)
//                        .build();
//                volumeBuffer.clear();
//                volumeDataHdfDataSource.writeToBuffer(instance, volumeBuffer);
//                volumeBuffer.flip();
//                return volumeBuffer;
//            });

            dataset.close();
            file.close();

            // auto close

            System.out.println("HDF5 file created and written successfully!");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void tryAllocator() {
        HdfAllocator allocator = new HdfAllocator(0);

        // Allocate Superblock
        long superblockOffset = allocator.allocate(56, 8);
        System.out.printf("Superblock at: %d\n", superblockOffset);

        // Allocate Symbol Table Entry
        long symbolTableEntryOffset = allocator.allocate(40, 8);
        System.out.printf("Symbol Table Entry at: %d\n", symbolTableEntryOffset);

        // Allocate Root Object Header
        long rootObjectHeaderOffset = allocator.allocate(584, 8);
        System.out.printf("Root Object Header at: %d\n", rootObjectHeaderOffset);

        // Allocate Local Heap
        long localHeapOffset = allocator.allocate(32, 8);
        System.out.printf("Local Heap at: %d\n", localHeapOffset);

        // Allocate Local Heap Contents
        long localHeapContentsOffset = allocator.allocate(88, 8);
        System.out.printf("Local Heap Contents at: %d\n", localHeapContentsOffset);

        // Allocate Dataset Object Header
        long datasetObjectHeaderOffset = allocator.allocate(1080, 8);
        System.out.printf("Dataset Object Header at: %d\n", datasetObjectHeaderOffset);

        // Allocate B-Trees
        long btreeOffset = allocator.allocate(328, 8);
        System.out.printf("B-Tree Structures at: %d\n", btreeOffset);

        // Align Dataset Storage to 2208
        long datasetStorageOffset = allocator.allocate(0, 2048); // Align to 2208 manually
        System.out.printf("Dataset Storage starts at: %d\n", datasetStorageOffset);

    }

    public void trySpliterator(FileChannel fileChannel, HdfReader reader) {

        FixedPointDataSource<TemperatureData> fixedPointDataSource = new FixedPointDataSource<>((FixedPointDatatype) reader.getDataType(), "temperature", TemperatureData.class);

        Spliterator<TemperatureData> spliterator = new HdfFixedPointDatatypeSpliterator<>(fileChannel, reader.getDataAddress(), reader.getDataType().getSize(), reader.getDimension(), fixedPointDataSource);

        System.out.println("count = " + StreamSupport.stream(spliterator, false).map(TemperatureData::getTemperature).collect(Collectors.summarizingInt(BigInteger::intValue)));


        spliterator.forEachRemaining(buffer -> {
            // Process each ByteBuffer (record) here
            System.out.println("Record: " + buffer);
        });

    }

//    public void tryHdfFileBuilder() throws IOException {
//        HdfGroupManager builder = new HdfGroupManager();
//
//        builder.superblock(4, 16, 0, 100320);
//
//        // Define a root group
//        builder.rootGroup(96);
//
//        builder.objectHeader();
//
//        // Define the heap data size
//        int dataSegmentSize = 88;
//
//        // Initialize the heapData array
//        byte[] heapData = new byte[dataSegmentSize];
//        Arrays.fill(heapData, (byte) 0); // Set all bytes to 0
//
//        // Set bytes 8-16 to the required values
//        heapData[8] = 68;  // 'D'
//        heapData[9] = 101; // 'e'
//        heapData[10] = 109; // 'm'
//        heapData[11] = 97; // 'a'
//        heapData[12] = 110; // 'n'
//        heapData[13] = 100; // 'd'
//        heapData[14] = 0;  // null terminator
//        heapData[15] = 0;  // additional null byte
//
//        builder.localHeap(dataSegmentSize, 16, 712, heapData);
//
//        // Define a dataset with correct CompoundDatatype members
//        // DatatypeMessage with CompoundDatatype
//        List<HdfCompoundDatatypeMember> members = List.of(
//                new HdfCompoundDatatypeMember("shipmentId", 0, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)8, false, false, false, false, (short)0, (short)64, computeFixedMessageDataSize("shipmentId"), new BitSet())),
//                new HdfCompoundDatatypeMember("origCountry", 8, 0, 0, new int[4],
//                        new StringDatatype((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origCountry"))),
//                new HdfCompoundDatatypeMember("origSlic", 10, 0, 0, new int[4],
//                        new StringDatatype((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origSlic"))),
//                new HdfCompoundDatatypeMember("origSort", 15, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("origSort"), new BitSet())),
//                new HdfCompoundDatatypeMember("destCountry", 16, 0, 0, new int[4],
//                        new StringDatatype((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destCountry"))),
//                new HdfCompoundDatatypeMember("destSlic", 18, 0, 0, new int[4],
//                        new StringDatatype((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destSlic"))),
//                new HdfCompoundDatatypeMember("destIbi", 23, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("destIbi"), new BitSet())),
//                new HdfCompoundDatatypeMember("destPostalCode", 40, 0, 0, new int[4],
//                        new StringDatatype((byte) 1, (short)9, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destPostalCode"))),
//                new HdfCompoundDatatypeMember("shipper", 24, 0, 0, new int[4],
//                        new StringDatatype((byte) 1, (short)10, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("shipper"))),
//                new HdfCompoundDatatypeMember("service", 49, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("service"), new BitSet())),
//                new HdfCompoundDatatypeMember("packageType", 50, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("packageType"), new BitSet())),
//                new HdfCompoundDatatypeMember("accessorials", 51, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("accessorials"), new BitSet())),
//                new HdfCompoundDatatypeMember("pieces", 52, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("pieces"), new BitSet())),
//                new HdfCompoundDatatypeMember("weight", 34, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("weight"), new BitSet())),
//                new HdfCompoundDatatypeMember("cube", 36, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)4, false, false, false, false, (short)0, (short)32, computeFixedMessageDataSize("cube"), new BitSet())),
//                new HdfCompoundDatatypeMember("committedTnt", 54, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedTnt"), new BitSet())),
//                new HdfCompoundDatatypeMember("committedDate", 55, 0, 0, new int[4],
//                        new FixedPointDatatype((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedDate"), new BitSet()))
//        );
//
//
//        List<HdfMessage> headerMessages = new ArrayList<>();
//        headerMessages.add(new ObjectHeaderContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
//        headerMessages.add(new NilMessage());
//
//        // Define Compound DataType correctly
//        short compoundSize = (short) members.stream().mapToInt(c->c.getType().getSize()).sum();
//        // Define Compound DataType correctly
//        CompoundDatatype compoundType = new CompoundDatatype(members.size(), compoundSize, members);
//        DatatypeMessage dataTypeMessage = new DatatypeMessage(1, 6, BitSet.valueOf(new byte[]{0b10001}), new HdfFixedPoint(false, new byte[]{(byte)56}, (short)4), compoundType);
////        dataTypeMessage.setDataType(compoundType);
//        headerMessages.add(dataTypeMessage);
//
//        // Add FillValue message
//        headerMessages.add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));
//
//        // Add DataLayoutMessage (Storage format)
//        HdfFixedPoint[] hdfDimensionSizes = { HdfFixedPoint.of(98000)};
//        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1, HdfFixedPoint.of(2208), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined((short)8));
//        headerMessages.add(dataLayoutMessage);
//
//        // add ObjectModification Time message
//        headerMessages.add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));
//
//        // Add DataspaceMessage (Handles dataset dimensionality)
//        HdfFixedPoint[] hdfDimensions = Arrays.stream(new long[]{1750}).mapToObj(HdfFixedPoint::of).toArray(HdfFixedPoint[]::new);
//        DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
//        headerMessages.add(dataSpaceMessage);
//
//        String attributeName = "GIT root revision";
//        String attributeValue = "Revision: , URL: ";
//        DatatypeMessage dt = new DatatypeMessage(1, 3, BitSet.valueOf(new byte[0]), HdfFixedPoint.of(attributeName.length()+1), new HdfString(attributeName, false));
//        DataspaceMessage ds = new DataspaceMessage(1, 1, 1, new HdfFixedPoint[] {HdfFixedPoint.of(1)}, null, false);
//        AttributeMessage attributeMessage = new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), new HdfString(attributeValue, false));
//        headerMessages.add(attributeMessage);
//
//        // new long[]{1750}, new long[]{98000}
//        builder.addDataset(headerMessages);
//
//        // Define a B-Tree for group indexing
//        builder.addBTree(1880, "Demand");
//
//        // Define a Symbol Table Node
//        builder.addSymbolTableNode(800);
//
//        // Write to an HDF5 file
//        builder.writeToFile(null);
//
//    }

    public short computeFixedMessageDataSize(String name) {
        if (name.length() > 0 ) {
            int padding = (8 -  ((name.length()+1)% 8)) % 8;
            return (short) (name.length()+1 + padding + 44);
        } else {
            return 44;
        }
    }

    public short computeStringMessageDataSize(String name) {
        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        return (short) (name.length()+1 + padding + 40);
    }

}
