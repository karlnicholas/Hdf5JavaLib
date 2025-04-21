package org.hdf5javalib.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.*;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.VariableLengthDatatype;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Getter
@Slf4j
public class HdfDataSet implements Closeable {
//    private final HdfGroup hdfGroup;
    private final HdfDataFile hdfDataFile;
    private final String datasetName;
    private final HdfDatatype hdfDatatype;
    private final List<AttributeMessage> attributes;
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;
    private boolean closed;

    /*
     * So, this is a group ?
     * it has a name, "Demand" off the root group of "/"
     * it has a datatype, "Compound" , with dimensions, attributes, and an address in the HdfFile
     * It should have a localHeap and LocalHeap contents, perhaps.
     */

    public HdfDataSet(HdfDataFile hdfDataFile, String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        this.hdfDataFile = hdfDataFile;
        this.datasetName = datasetName;
        this.hdfDatatype = hdfDatatype;
        this.attributes = new ArrayList<>();
        closed = false;
        createInitialMessages(dataSpaceMessage);
    }

    public HdfDataSet(HdfDataFile hdfDataFile, String datasetName, HdfDatatype hdfDatatype, HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix) {
        this.hdfDataFile = hdfDataFile;
        this.datasetName = datasetName;
        this.hdfDatatype = hdfDatatype;
        this.attributes = new ArrayList<>();
        this.dataObjectHeaderPrefix = dataObjectHeaderPrefix;
        closed = false;
        dataObjectHeaderPrefix.findMessageByType(AttributeMessage.class).ifPresent(attributes::add);
    }

    private void createInitialMessages(DataspaceMessage dataSpaceMessage) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);

        // int headerSize = hdfGroup.getHdfFile().getBufferAllocation().getDataGroupStorageSize();
        long headerSize = allocationInfo.getHeaderSize();
        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(dataSpaceMessage);

        // So, why, compoundMessageType size is OK but for fixed it needs + 8
//        short dataTypeMessageSize = 8;
        short dataTypeMessageSize = 0;
        dataTypeMessageSize += hdfDatatype.getSizeMessageData();
        // to 8 byte boundary
        dataTypeMessageSize = (short) ((dataTypeMessageSize + 7) & ~7);
        DatatypeMessage dataTypeMessage = new DatatypeMessage(hdfDatatype, (byte)1, dataTypeMessageSize);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        // not sure what's going on with fillValueWriteTime.
        /**
         * At the time that storage space for the dataset’s raw data is allocated,
         * this value indicates whether the fill value should be written to the
         * raw data storage elements. The allowed values are:
         *
         * Value | Description
         * ------|---------------------------------------------------------------
         *   0   | On allocation. The fill value is always written to the
         *       | raw data storage when the storage space is allocated.
         * ------|---------------------------------------------------------------
         *   1   | Never. The fill value should never be written to the
         *       | raw data storage.
         * ------|---------------------------------------------------------------
         *   2   | Fill value written if set by user. The fill value will be
         *       | written to the raw data storage only if the user explicitly
         *       | set the fill value. If the fill value is the library default
         *       | or is undefined, it will not be written.
         */
        int fillValueWriteTime = 2;
        if ( hdfDatatype.getDatatypeClass() == HdfDatatype.DatatypeClass.COMPOUND) {
            fillValueWriteTime = 0;
        }
        FillValueMessage fillValueMessage = new FillValueMessage(2, 2, fillValueWriteTime, 1,
                0,
                new byte[0], (byte)1, (short)8);
        headerMessages.add(fillValueMessage);

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] dimensions = dataSpaceMessage.getDimensions();
//        long recordCount = dimensions[dimensions.length-1].toBigInteger().longValue();
        long dimensionSizes = hdfDatatype.getSize();
        for(HdfFixedPoint fixedPoint : dimensions) {
            dimensionSizes *= fixedPoint.getInstance(Long.class);
        }
        HdfFixedPoint[] hdfDimensionSizes = (HdfFixedPoint[]) Array.newInstance(HdfFixedPoint.class, 1);
        hdfDimensionSizes[0] = HdfFixedPoint.of(dimensionSizes);


        short dataLayoutMessageSize = (short) 8;
        switch (1) {
            case 0: // Compact Storage
                break;

            case 1: // Contiguous Storage
                dataLayoutMessageSize += 16;
                break;

            case 2: // Chunked Storage
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + 1);
        }

        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1,
                // HdfFixedPoint.of(hdfGroup.getHdfFile().getBufferAllocation().getDataAddress()),
                HdfFixedPoint.of(allocationInfo.getDataOffset()),
                hdfDimensionSizes,
                0, null, HdfFixedPoint.undefined((short)8), (byte)0, dataLayoutMessageSize);
        headerMessages.add(dataLayoutMessage);

        // add ObjectModification Time message
        ObjectModificationTimeMessage objectModificationTimeMessage = new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond(), (byte)0, (short)8);
        headerMessages.add(objectModificationTimeMessage);

        // attribute messages at the end
        headerMessages.addAll(attributes);

        int objectReferenceCount = 1;
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        if ( objectHeaderSize > headerSize-16) {
            // headerSize = hdfGroup.getHdfFile().getBufferAllocation().expandDataGroupStorageSize(objectHeaderSize);
            fileAllocation.increaseHeaderAllocation(datasetName, objectHeaderSize+16);
            // update
            allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
            headerSize = allocationInfo.getHeaderSize();
        }
        // redo addresses already set.
        // dataLayoutMessage.setDataAddress(HdfFixedPoint.of(hdfGroup.getHdfFile().getBufferAllocation().getDataAddress()));
        dataLayoutMessage.setDataAddress(HdfFixedPoint.of(allocationInfo.getDataOffset()));
        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, objectReferenceCount, Math.max(objectHeaderSize, headerSize-16), headerMessages);
//        hdfGroup.getHdfFile().recomputeGlobalHeapAddress(this);
        // check if a global heap needed
    }

    public AttributeMessage createAttribute(String name, String value, HdfDataFile hdfDataFile) {
        boolean requiresGlobalHeap = hdfDatatype.requiresGlobalHeap(false);

        // Create datatype and messages
        HdfDatatype attributeType = createAttributeType(requiresGlobalHeap, value);
        DatatypeMessage datatypeMessage = createDatatypeMessage(attributeType);
        DataspaceMessage dataspaceMessage = createDataspaceMessage();

        // Calculate sizes with 8-byte alignment
        int nameSize = alignTo8Bytes(name.length());
        int valueSize = calculateValueSize(value, requiresGlobalHeap);

        int attributeMessageSize = requiresGlobalHeap ? 12 : 8;
        attributeMessageSize = attributeMessageSize + nameSize + datatypeMessage.getSizeMessageData() +
                dataspaceMessage.getSizeMessageData() + valueSize;

        // Create and register attribute message
        AttributeMessage attributeMessage = new AttributeMessage(
                1,
                new HdfString(name.getBytes(StandardCharsets.US_ASCII),
                        new StringDatatype(StringDatatype.createClassAndVersion(),
                                StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE,
                                        StringDatatype.CharacterSet.ASCII), name.length() + 1)),
                datatypeMessage,
                dataspaceMessage,
                null,
                (byte) 0,
                (short) attributeMessageSize
        );

        attributes.add(attributeMessage);
        updateForAttribute();

        // Set attribute value
        HdfData attributeValue = createAttributeValue(value, attributeType, requiresGlobalHeap, hdfDataFile);
        attributeMessage.setValue(attributeValue);

        return attributeMessage;
    }

    private HdfDatatype createAttributeType(boolean requiresGlobalHeap, String value) {
        if (requiresGlobalHeap) {
            FixedPointDatatype fixedType = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField(false, false, false, false),
                    1, (short) 0, (short) 8
            );
            VariableLengthDatatype variableLengthType = new VariableLengthDatatype(
                    VariableLengthDatatype.createClassAndVersion(),
                    VariableLengthDatatype.createClassBitField(VariableLengthDatatype.Type.STRING, VariableLengthDatatype.PaddingType.NULL_TERMINATE,
                            VariableLengthDatatype.CharacterSet.ASCII),
                    (short) 16, fixedType
            );
            return variableLengthType;
        }
        return new StringDatatype(
                StringDatatype.createClassAndVersion(),
                StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE,
                        StringDatatype.CharacterSet.ASCII),
                (short) value.length()
        );
    }

    private DatatypeMessage createDatatypeMessage(HdfDatatype attributeType) {
//        short dataTypeMessageSize = (short) (8 + attributeType.getSizeMessageData());
//        dataTypeMessageSize = alignTo8Bytes(dataTypeMessageSize);
        short dataTypeMessageSize = 0;
        dataTypeMessageSize += attributeType.getSizeMessageData();
        // to 8 byte boundary
//        dataTypeMessageSize = (short) ((dataTypeMessageSize + 7) & ~7);
        return new DatatypeMessage(attributeType, (byte) 1, dataTypeMessageSize);
    }

    private DataspaceMessage createDataspaceMessage() {
        HdfFixedPoint[] hdfDimensions = {};
        short dataSpaceMessageSize = 8;
        return new DataspaceMessage(
                1, 0, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false),
                null, null, false, (byte) 0, dataSpaceMessageSize
        );
    }

    private HdfData createAttributeValue(String value, HdfDatatype attributeType,
                                         boolean requiresGlobalHeap, HdfDataFile hdfDataFile) {
        if (requiresGlobalHeap) {
            VariableLengthDatatype variableLengthType = new VariableLengthDatatype(
                    VariableLengthDatatype.createClassAndVersion(),
                    VariableLengthDatatype.createClassBitField(VariableLengthDatatype.Type.STRING, VariableLengthDatatype.PaddingType.NULL_TERMINATE,
                            VariableLengthDatatype.CharacterSet.ASCII),
                    (short) 16, attributeType
            );
            hdfDataFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            variableLengthType.setGlobalHeap(hdfDataFile.getGlobalHeap());
            byte[] globalHeapBytes = hdfDataFile.getGlobalHeap().addToHeap(
                    value.getBytes(StandardCharsets.US_ASCII));
            return new HdfVariableLength(globalHeapBytes, variableLengthType);
        }
        return new HdfString(value.getBytes(StandardCharsets.US_ASCII), (StringDatatype) attributeType);
    }

    private short alignTo8Bytes(int size) {
        return (short) ((size + 7) & ~7);
    }

    private int calculateValueSize(String value, boolean requiresGlobalHeap) {
        if (requiresGlobalHeap) {
            return 16; // + 16; // Fixed size for global heap: 16 (address) + 16 (size)
        }
        int length = (value != null) ? value.length() : 0;
        return alignTo8Bytes(length);
    }

    /**
     * See if attribute fits in current objectHeader storage and if not
     * indicate continutationMessage requirements
     */
    private void updateForAttribute() {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        //  int headerSize = hdfGroup.getHdfFile().getBufferAllocation().getDataGroupStorageSize();
        long headerSize = allocationInfo.getHeaderSize();
        List<HdfMessage> headerMessages = this.dataObjectHeaderPrefix.getHeaderMessages();
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        int attributeSize = getAttributeSize();

        checkContinuationMessageNeeded(objectHeaderSize, attributeSize, headerMessages, headerSize-16);

    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        if (hdfDataFile.getFileAllocation().getDatasetAllocationInfo(datasetName) == null) return;
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        // int headerSize = hdfGroup.getHdfFile().getBufferAllocation().getDataGroupStorageSize();
        long headerSize = allocationInfo.getHeaderSize();
        List<HdfMessage> headerMessages = this.dataObjectHeaderPrefix.getHeaderMessages();
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        int attributeSize = getAttributeSize();

        if ( checkContinuationMessageNeeded(objectHeaderSize, attributeSize, headerMessages, headerSize-16)) {
            // needs to be updated? I think so.
            allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
            // restructure the messages
            List<HdfMessage> updatedHeaderMessages = new ArrayList<>();

            // if you take out the dataSpaceMessage and replace it with
            // a objectHeaderContinuationMessage and NilMessage then the size is the same
            ObjectHeaderContinuationMessage objectHeaderContinuationMessage = new ObjectHeaderContinuationMessage(HdfFixedPoint.of(0), HdfFixedPoint.of(0), (byte)0, (short)16);
            updatedHeaderMessages.add(objectHeaderContinuationMessage);
            // NiLMessage is now 0 size because there is no extra space
            updatedHeaderMessages.add(new NilMessage(0, (byte)0, (short)0));
            HdfMessage dataSpaceMessage = headerMessages.remove(0);
            if ( !(dataSpaceMessage instanceof DataspaceMessage) ) {
                throw new IllegalStateException("Find DataspaceMessage for " + datasetName);
            }
            updatedHeaderMessages.addAll(headerMessages);

            // continuation messages
            updatedHeaderMessages.add(dataSpaceMessage);
            updatedHeaderMessages.addAll(attributes);
            attributes.clear();
//            int continueSize = dataSpaceMessage.getSizeMessageData() + 8 + attributeSize;
            headerMessages.clear();
            headerMessages.addAll(updatedHeaderMessages);

            objectHeaderContinuationMessage.setContinuationOffset(HdfFixedPoint.of(allocationInfo.getContinuationOffset()));
            objectHeaderContinuationMessage.setContinuationSize(HdfFixedPoint.of(allocationInfo.getContinuationSize()));
            // set the object header size.
        } else if ( objectHeaderSize + attributeSize  < headerSize-16 ) {
            if ( !attributes.isEmpty() ) {
                headerMessages.addAll(attributes);
                attributes.clear();
            }
            long nilSize = (headerSize - 16) - (objectHeaderSize + attributeSize) - 8;
            headerMessages.add(new NilMessage((int) nilSize, (byte)0, (short)nilSize));
        }
        DataLayoutMessage dataLayoutMessage = dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class).orElseThrow();
        // redo addresses already set.
        // dataLayoutMessage.setDataAddress(HdfFixedPoint.of(hdfGroup.getHdfFile().getBufferAllocation().getDataAddress()));
        dataLayoutMessage.setDataAddress(HdfFixedPoint.of(allocationInfo.getDataOffset()));
//        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, headerMessages.size(), objectReferenceCount, Math.max(objectHeaderSize, headerSize), headerMessages);
//        hdfGroup.getHdfFile().recomputeGlobalHeapAddress(this);
        // need to write the dataset
        writeToFileChannel(hdfDataFile.getSeekableByteChannel());

        closed = true;
    }

    private boolean checkContinuationMessageNeeded(int objectHeaderSize, int attributeSize, List<HdfMessage> headerMessages, long headerSize) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        if ( objectHeaderSize + attributeSize > headerSize
                && (allocationInfo.getContinuationSize() <= 0 || !attributes.isEmpty())
        ) {
            HdfMessage dataspaceMessage = headerMessages.get(0);
            if ( !(dataspaceMessage instanceof DataspaceMessage)) {
                throw new IllegalArgumentException("Dataspace message not found: " + dataspaceMessage.getClass().getName());
            }
            // hdfGroup.getHdfFile().getBufferAllocation().setDataGroupAndContinuationStorageSize(headerSize, dataspaceMessage.getSizeMessageData() + 8 + attributeSize);
            // New code:
//            long newStorageSize = headerSize;
            if ( allocationInfo.getHeaderSize() < headerSize ) {
                fileAllocation.increaseHeaderAllocation(datasetName, headerSize);
            }
            int newContinuationSize = dataspaceMessage.getSizeMessageData() + 8 + attributeSize; // Calculate size first for clarity
            if ( allocationInfo.getContinuationSize() < newContinuationSize ) {
                fileAllocation.allocateAndSetContinuationBlock(datasetName, newContinuationSize);
            }
            // set the object header size.
            // redo addresses already set.
//            hdfGroup.getHdfFile().recomputeGlobalHeapAddress(this);
            return true;
        }
        return false;
    }

    private int getAttributeSize() {
        int attributeSize = 0;
        for (HdfMessage hdfMessage : attributes) {
            log.trace("Write: hdfMessage.getSizeMessageData() + 8 = {} {}", hdfMessage.getMessageType(), hdfMessage.getSizeMessageData() + 8);
            attributeSize += hdfMessage.getSizeMessageData() + 8;
        }
        return attributeSize;
    }

    private int getObjectHeaderSize(List<HdfMessage> headerMessages) {
        int objectHeaderSize = 0;
        for( HdfMessage hdfMessage: headerMessages) {
            log.trace("Write: hdfMessage.getSizeMessageData() + 8 = {} {}", hdfMessage.getMessageType(), hdfMessage.getSizeMessageData() + 8);
            objectHeaderSize += hdfMessage.getSizeMessageData() + 8;
        }
        return objectHeaderSize;
    }

//    /**
//     * For writing data to the dataset.
//     * @param bufferSupplier
//     * @throws IOException
//     */
//    public void write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
//        hdfDataFile.write(bufferSupplier, this);
//    }
//
//    /**
//     * For writing data to the dataset.
//     * @param byteBuffer
//     * @throws IOException
//     */
//    public void write(ByteBuffer byteBuffer) throws IOException {
//        hdfDataFile.write(byteBuffer, this);
//    }
    public void write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        DatasetAllocationInfo allocationInfo = hdfDataFile.getFileAllocation().getDatasetAllocationInfo(datasetName);
        hdfDataFile.getSeekableByteChannel().position(allocationInfo.getDataOffset());
        ByteBuffer buffer;
        while ((buffer = bufferSupplier.get()).hasRemaining()) {
            while (buffer.hasRemaining()) {
                hdfDataFile.getSeekableByteChannel().write(buffer);
            }
        }
    }

    public void write(ByteBuffer buffer) throws IOException {
        DatasetAllocationInfo allocationInfo = hdfDataFile.getFileAllocation().getDatasetAllocationInfo(datasetName);
        hdfDataFile.getSeekableByteChannel().position(allocationInfo.getDataOffset());
        while (buffer.hasRemaining()) {
            hdfDataFile.getSeekableByteChannel().write(buffer);
        }
    }


    public HdfFixedPoint getDataAddress() {
        return dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class).orElseThrow().getDataAddress();
    }

    public HdfFixedPoint[] getdimensionSizes() {
        return dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class).orElseThrow().getDimensionSizes();
    }

    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        ByteBuffer buffer = ByteBuffer.allocate((int)allocationInfo.getHeaderSize()).order(ByteOrder.LITTLE_ENDIAN);
        dataObjectHeaderPrefix.writeInitialMessageBlockToBuffer(buffer);
        buffer.rewind();

        fileChannel.position(allocationInfo.getHeaderOffset());
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        if ( allocationInfo.getContinuationOffset() > 0 ) {
            buffer = ByteBuffer.allocate((int)allocationInfo.getContinuationSize()).order(ByteOrder.LITTLE_ENDIAN);
            dataObjectHeaderPrefix.writeContinuationMessageBlockToBuffer((int)allocationInfo.getHeaderSize(), buffer);
            buffer.flip();

            fileChannel.position(allocationInfo.getContinuationOffset());
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        }

    }

}
