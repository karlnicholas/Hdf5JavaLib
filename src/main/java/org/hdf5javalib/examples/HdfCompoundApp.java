package org.hdf5javalib.examples;

import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.DataClassDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundMemberDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class HdfCompoundApp {
    public static void main(String[] args) {
        new HdfCompoundApp().run();
    }
    private void run() {
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = HdfCompoundApp.class.getResource("/test.h5").getFile();
            try(FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                tryCompoundSpliterator(channel, reader);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        tryHdfApiCompound();
//        tryHdfApiInts();
    }

    private void writeVersionAttribute(HdfDataSet dataset) {
        String ATTRIBUTE_NAME = "GIT root revision";
        String ATTRIBUTE_VALUE = "Revision: , URL: ";
        BitSet classBitField = StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII);
        // value
        StringDatatype attributeType = new StringDatatype(StringDatatype.createClassAndVersion(), classBitField, (short)ATTRIBUTE_VALUE.length());
        // data type, String, DATASET_NAME.length
        DatatypeMessage dt = new DatatypeMessage(attributeType);
        // scalar, 1 string
        DataspaceMessage ds = new DataspaceMessage(1, 0, 0, null, null, false);
        HdfString hdfString = new HdfString(ATTRIBUTE_VALUE.getBytes(), classBitField);
        dataset.createAttribute(ATTRIBUTE_NAME, dt, ds, hdfString);
    }

    public void tryHdfApiCompound() {
        final String FILE_NAME = "testcompound.h5";
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "Demand";
        final int NUM_RECORDS = 1750;

        try {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(FILE_NAME, FILE_OPTIONS);
            // DatatypeMessage with CompoundDatatype
            BitSet stringBitSet = StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII);
            List<CompoundMemberDatatype> shipment = List.of(
                    new CompoundMemberDatatype("shipmentId", 0, 0, 0, new int[4],
                            new FixedPointDatatype(
                            FixedPointDatatype.createClassAndVersion(),
                            FixedPointDatatype.createClassBitField( false, false, false, false),
                            (short)8, (short)0, (short)64)),
                    new CompoundMemberDatatype("origCountry", 8, 0, 0, new int[4],
                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short)2)),
                    new CompoundMemberDatatype("origSlic", 10, 0, 0, new int[4],
                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short)5)),
                    new CompoundMemberDatatype("origSort", 15, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)1, (short)0, (short)8)),
                    new CompoundMemberDatatype("destCountry", 16, 0, 0, new int[4],
                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short)2)),
                    new CompoundMemberDatatype("destSlic", 18, 0, 0, new int[4],
                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short)5)),
                    new CompoundMemberDatatype("destIbi", 23, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)1, (short)0, (short)8)),
                    new CompoundMemberDatatype("destPostalCode", 24, 0, 0, new int[4],
                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short)9)),
                    new CompoundMemberDatatype("shipper", 33, 0, 0, new int[4],
                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short)10)),
                    new CompoundMemberDatatype("service", 43, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)1, (short)0, (short)8)),
                    new CompoundMemberDatatype("packageType", 44, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)1, (short)0, (short)8)),
                    new CompoundMemberDatatype("accessorials", 45, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)1, (short)0, (short)8)),
                    new CompoundMemberDatatype("pieces", 46, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)2, (short)0, (short)16)),
                    new CompoundMemberDatatype("weight", 48, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)2, (short)0, (short)16)),
                    new CompoundMemberDatatype("cube", 50, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)4, (short)0, (short)32)),
                    new CompoundMemberDatatype("committedTnt", 54, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)1, (short)0, (short)8)),
                    new CompoundMemberDatatype("committedDate", 55, 0, 0, new int[4],
                            new FixedPointDatatype(
                                    FixedPointDatatype.createClassAndVersion(),
                                    FixedPointDatatype.createClassBitField( false, false, false, false),
                                    (short)1, (short)0, (short)8))
            );
            short compoundSize = (short) shipment.stream().mapToInt(c->c.getType().getSize()).sum();
            // Define Compound DataType correctly
            CompoundDatatype compoundType = new CompoundDatatype(CompoundDatatype.createClassAndVersion(), CompoundDatatype.createClassBitField((short) shipment.size()), compoundSize, shipment);
//            DatatypeMessage dataTypeMessage = new DatatypeMessage((byte) 1, (byte) 6, BitSet.valueOf(new byte[]{0b10001}), 56, compoundType);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(NUM_RECORDS)};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, false);
//            hsize_t dim[1] = { NUM_RECORDS };
//            DataSpace space(1, dim);

            // Create dataset
//            DataSet dataset = file.createDataSet(DATASET_NAME, compoundType, space);
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, compoundType, dataSpaceMessage);


            // ADD ATTRIBUTE: "GIT root revision"
            writeVersionAttribute(dataset);

//            AtomicInteger countHolder = new AtomicInteger(0);
//            TypedCompoundDataSource<ShipperData> volumeDataHdfDataSource = new TypedCompoundDataSource<>(compoundType, ShipperData.class);
//            ByteBuffer volumeBuffer = ByteBuffer.allocate(compoundType.getSize());
//            // Write to dataset
//            dataset.write(() -> {
//                int count = countHolder.getAndIncrement();
//                if (count >= NUM_RECORDS) return  ByteBuffer.allocate(0);
//                ShipperData instance = ShipperData.builder()
//                        .shipmentId(BigInteger.valueOf(count + 1000))
//                        .origCountry("US")
//                        .origSlic("12345")
//                        .origSort(BigInteger.valueOf(4))
//                        .destCountry("CA")
//                        .destSlic("67890")
//                        .destIbi(BigInteger.ZERO)
//                        .destPostalCode("A1B2C3")
//                        .shipper("FedEx")
//                        .service(BigInteger.ZERO)
//                        .packageType(BigInteger.valueOf(3))
//                        .accessorials(BigInteger.ZERO)
//                        .pieces(BigInteger.valueOf(2))
//                        .weight(BigInteger.valueOf(50))
//                        .cube(BigInteger.valueOf(1200))
//                        .committedTnt(BigInteger.valueOf(255))
//                        .committedDate(BigInteger.valueOf(3))
//                        .build();
//                volumeBuffer.clear();
//                volumeDataHdfDataSource.writeToBuffer(instance, volumeBuffer);
//                volumeBuffer.position(0);
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


    public void tryCompoundSpliterator(FileChannel fileChannel, HdfFileReader reader) throws IOException {
//        TypedCompoundDataSource<ShipperData> dataSource = new TypedCompoundDataSource<>((CompoundDatatype) reader.getDataType(), ShipperData.class);
//        Spliterator<ShipperData> spliterator = new CompoundDatatypeSpliterator<>(fileChannel, reader.getDataAddress(), reader.getDataType().getSize(), reader.getDimension(), dataSource);
//        System.out.println("count = " + StreamSupport.stream(spliterator, false).map(ShipperData::getPieces).collect(Collectors.summarizingInt(BigInteger::intValue)));
//        TypedDataClassDataSource<ShipperData> dataSource = new TypedDataClassDataSource<>(reader.getDataObjectHeaderPrefix(), "temperature", 0, ShipperData.class, fileChannel, reader.getDataAddress());
//        System.out.println("count = " + dataSource.stream().map(ShipperData::getPieces).collect(Collectors.summarizingInt(BigInteger::intValue)));
        DataClassDataSource<HdfCompound> dataSource = new DataClassDataSource<>(reader.getDataObjectHeaderPrefix(), 0, fileChannel, reader.getDataAddress(), HdfCompound.class);
        HdfCompound[] allData = dataSource.readAll();
        System.out.println("readAll stats = " + Arrays.stream(allData)
                .map(c->c.getMembers().stream()
                        .filter(m->m.getName().equalsIgnoreCase("pieces")).findFirst().orElseThrow())
                .map(cm->((HdfFixedPoint)cm.getData()).toBigInteger())
                .collect(Collectors.summarizingInt(BigInteger::intValue)));

        System.out.println("stream stats = " + dataSource.stream()
                .map(c->c.getMembers().stream()
                        .filter(m->m.getName().equalsIgnoreCase("pieces")).findFirst().orElseThrow())
                .map(cm->((HdfFixedPoint)cm.getData()).toBigInteger())
                .collect(Collectors.summarizingInt(BigInteger::intValue)));

        System.out.println("parallelStream stats = " + dataSource.parallelStream()
                .map(c->c.getMembers().stream()
                        .filter(m->m.getName().equalsIgnoreCase("pieces")).findFirst().orElseThrow())
                .map(cm->((HdfFixedPoint)cm.getData()).toBigInteger())
                .collect(Collectors.summarizingInt(BigInteger::intValue)));
    }
}
