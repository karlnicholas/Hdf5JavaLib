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
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
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
            String filePath = App.class.getResource("/test.h5").getFile();
//            String filePath = App.class.getResource("/ExportedNodeShips.h5").getFile();
            try(FileInputStream fis = new FileInputStream(new File(filePath))) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
//                printData(channel, reader.getCompoundDataType(), reader.getDataAddress(), reader.getDimension());
//                trySpliterator(channel, reader);
//                new HdfConstruction().buildHfd();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        tryHdfFileBuilder();
    }

    public void tryHdfApi() {
        final String FILE_NAME = "test.h5";
        final String DATASET_NAME = "Demand";
        final String ATTRIBUTE_NAME = "GIT root revision";
        StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};
        final int NUM_RECORDS = 1750;

        try {
            // Create a new HDF5 file
            H5File file = new H5File(FILE_NAME, FILE_OPTIONS);

            // DataTypeMessage with CompoundDataType
            List<CompoundDataType.Member> shipment = List.of(
                    new CompoundDataType.Member("shipmentId", 0, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)8, false, false, false, false, (short)0, (short)64, computeFixedMessageDataSize("shipmentId"), new BitSet())),
                    new CompoundDataType.Member("origCountry", 8, 0, 0, new int[4],
                            new CompoundDataType.StringMember((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origCountry"))),
                    new CompoundDataType.Member("origSlic", 10, 0, 0, new int[4],
                            new CompoundDataType.StringMember((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origSlic"))),
                    new CompoundDataType.Member("origSort", 15, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("origSort"), new BitSet())),
                    new CompoundDataType.Member("destCountry", 16, 0, 0, new int[4],
                            new CompoundDataType.StringMember((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destCountry"))),
                    new CompoundDataType.Member("destSlic", 18, 0, 0, new int[4],
                            new CompoundDataType.StringMember((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destSlic"))),
                    new CompoundDataType.Member("destIbi", 23, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("destIbi"), new BitSet())),
                    new CompoundDataType.Member("destPostalCode", 40, 0, 0, new int[4],
                            new CompoundDataType.StringMember((byte) 1, (short)9, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destPostalCode"))),
                    new CompoundDataType.Member("shipper", 24, 0, 0, new int[4],
                            new CompoundDataType.StringMember((byte) 1, (short)10, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("shipper"))),
                    new CompoundDataType.Member("service", 49, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("service"), new BitSet())),
                    new CompoundDataType.Member("packageType", 50, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("packageType"), new BitSet())),
                    new CompoundDataType.Member("accessorials", 51, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("accessorials"), new BitSet())),
                    new CompoundDataType.Member("pieces", 52, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("pieces"), new BitSet())),
                    new CompoundDataType.Member("weight", 34, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("weight"), new BitSet())),
                    new CompoundDataType.Member("cube", 36, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)4, false, false, false, false, (short)0, (short)32, computeFixedMessageDataSize("cube"), new BitSet())),
                    new CompoundDataType.Member("committedTnt", 54, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedTnt"), new BitSet())),
                    new CompoundDataType.Member("committedDate", 55, 0, 0, new int[4],
                            new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedDate"), new BitSet()))
            );
            short compoundSize = (short) shipment.stream().mapToInt(c->c.getType().getSize()).sum();
            // Define Compound DataType correctly
            CompoundDataType compoundType = new CompoundDataType(shipment.size(), compoundSize, shipment);
//            DataTypeMessage dataTypeMessage = new DataTypeMessage(1, 6, BitSet.valueOf(new byte[]{0b10001}), new HdfFixedPoint(false, new byte[]{(byte)56}, (short)4), compoundType);

            // ✅ Create data space
            HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(NUM_RECORDS)};
//            DataSpaceMessage dataSpaceMessage = new DataSpaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
//            hsize_t dim[1] = { NUM_RECORDS };
//            DataSpace space(1, dim);

            // ✅ Create dataset
//            DataSet dataset = file.createDataSet(DATASET_NAME, compoundType, space);
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, compoundType, hdfDimensions);


            // ✅ ADD ATTRIBUTE: "GIT root revision"
            String attributeValue = "Revision: , URL: ";
            HdfFixedPoint attr_type = HdfFixedPoint.of(ATTRIBUTE_NAME.length()+1);
            HdfFixedPoint[] attr_space = new HdfFixedPoint[] {HdfFixedPoint.of(1)};
            AttributeMessage attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
            attribute.write(attr_type, attributeValue);
            attribute.close();

//            H5std_string attribute_value = "Revision: , URL: ";
//            StrType attr_type(PredType::C_S1, attribute_value.size());
//            DataSpace attr_space(H5S_SCALAR);
//            Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
//            attribute.write(attr_type, attribute_value);
//            attribute.close();

            // ✅ Allocate a vector of `Shipment` structs
            List<VolumeData> shipments = new ArrayList<>(NUM_RECORDS);

            // ✅ Fill the struct array
            for (int i = 0; i < NUM_RECORDS; i++) {
                shipments.add(
                    VolumeData.builder()
                            .shipmentId(BigInteger.valueOf(i+1000))
                            .origCountry("US")
                            .origSlic("1234")
                            .origSort(BigInteger.valueOf(4))
                            .destCountry("US")
                            .destSlic("4321")
                            .destIbi(BigInteger.ZERO)
                            .destPostalCode("94211")
                            .shipper("DexEf")
                            .packageType(BigInteger.ONE)
                            .accessorials(BigInteger.ZERO)
                            .pieces(BigInteger.valueOf(10))
                            .pieces(BigInteger.valueOf(50))
                            .cube(BigInteger.valueOf(1200))
                            .committedTnt(BigInteger.ZERO)
                            .committedDate(BigInteger.ZERO)
                            .build()
                );
            }

            // ✅ Write to dataset
            dataset.write(shipments, compoundType, VolumeData.class);

            // ✅ Close resources
            dataset.close();
            file.close();

            System.out.println("HDF5 file created and written successfully!");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void tryAllocator() {
        Hdf5Allocator allocator = new Hdf5Allocator(0);

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

        HdfDataSource<VolumeData> hdfDataSource = new HdfDataSource(reader.getCompoundDataType(), VolumeData.class);

        Spliterator<VolumeData> spliterator = new HdfSpliterator(fileChannel, reader.getDataAddress(), reader.getCompoundDataType().getSize(), reader.getDimension(), hdfDataSource);

        System.out.println("count = " + StreamSupport.stream(spliterator, false).map(VolumeData::getPieces).collect(Collectors.summarizingInt(BigInteger::intValue)));


//        spliterator.forEachRemaining(buffer -> {
//            // Process each ByteBuffer (record) here
//            System.out.println("Record: " + buffer);
//        });

    }

    public void tryHdfFileBuilder() {
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
                new CompoundDataType.Member("shipmentId", 0, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)8, false, false, false, false, (short)0, (short)64, computeFixedMessageDataSize("shipmentId"), new BitSet())),
                new CompoundDataType.Member("origCountry", 8, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origCountry"))),
                new CompoundDataType.Member("origSlic", 10, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("origSlic"))),
                new CompoundDataType.Member("origSort", 15, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("origSort"), new BitSet())),
                new CompoundDataType.Member("destCountry", 16, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte) 1, (short)2, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destCountry"))),
                new CompoundDataType.Member("destSlic", 18, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte) 1, (short)5, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destSlic"))),
                new CompoundDataType.Member("destIbi", 23, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("destIbi"), new BitSet())),
                new CompoundDataType.Member("destPostalCode", 40, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte) 1, (short)9, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("destPostalCode"))),
                new CompoundDataType.Member("shipper", 24, 0, 0, new int[4],
                        new CompoundDataType.StringMember((byte) 1, (short)10, 0, "Null Terminate", 0, "ASCII", computeStringMessageDataSize("shipper"))),
                new CompoundDataType.Member("service", 49, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("service"), new BitSet())),
                new CompoundDataType.Member("packageType", 50, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("packageType"), new BitSet())),
                new CompoundDataType.Member("accessorials", 51, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("accessorials"), new BitSet())),
                new CompoundDataType.Member("pieces", 52, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("pieces"), new BitSet())),
                new CompoundDataType.Member("weight", 34, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)2, false, false, false, false, (short)0, (short)16, computeFixedMessageDataSize("weight"), new BitSet())),
                new CompoundDataType.Member("cube", 36, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)4, false, false, false, false, (short)0, (short)32, computeFixedMessageDataSize("cube"), new BitSet())),
                new CompoundDataType.Member("committedTnt", 54, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedTnt"), new BitSet())),
                new CompoundDataType.Member("committedDate", 55, 0, 0, new int[4],
                        new CompoundDataType.FixedPointMember((byte) 1, (short)1, false, false, false, false, (short)0, (short)8, computeFixedMessageDataSize("committedDate"), new BitSet()))
        );


        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(new ContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
        headerMessages.add(new NullMessage());

        // Define Compound DataType correctly
        short compoundSize = (short) members.stream().mapToInt(c->c.getType().getSize()).sum();
        // Define Compound DataType correctly
        CompoundDataType compoundType = new CompoundDataType(members.size(), compoundSize, members);
        DataTypeMessage dataTypeMessage = new DataTypeMessage(1, 6, BitSet.valueOf(new byte[]{0b10001}), new HdfFixedPoint(false, new byte[]{(byte)56}, (short)4), compoundType);
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

        String attributeName = "GIT root revision";
        String attributeValue = "Revision: , URL: ";
        DataTypeMessage dt = new DataTypeMessage(1, 3, BitSet.valueOf(new byte[0]), HdfFixedPoint.of(attributeName.length()+1), new HdfString(attributeName, false));
        DataSpaceMessage ds = new DataSpaceMessage(1, 1, 1, new HdfFixedPoint[] {HdfFixedPoint.of(1)}, null, false);
        headerMessages.add(new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), new HdfString(attributeValue, false)));

        // new long[]{1750}, new long[]{98000}
        builder.addDataset(headerMessages);

        // Define a B-Tree for group indexing
        builder.addBTree(1880, "Demand");

        // Define a Symbol Table Node
        builder.addSymbolTableNode(800);

        // Write to an HDF5 file
        builder.writeToFile("output.hdf5");

    }

    public short computeFixedMessageDataSize(String name) {
        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        return (short) (name.length()+1 + padding + 44);
    }

    public short computeStringMessageDataSize(String name) {
        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        return (short) (name.length()+1 + padding + 40);
    }

}
