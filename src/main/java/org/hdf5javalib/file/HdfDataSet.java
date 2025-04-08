package org.hdf5javalib.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.*;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@Getter
@Slf4j
public class HdfDataSet implements Closeable {
    private final HdfGroup hdfGroup;
    private final String datasetName;
    private final HdfDatatype hdfDatatype;
    private final List<AttributeMessage> attributes;
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;

    /*
     * So, this is a group ?
     * it has a name, "Demand" off the root group of "/"
     * it has a datatype, "Compound" , with dimensions, attributes, and an address in the HdfFile
     * It should have a localHeap and LocalHeap contents, perhaps.
     */

    public HdfDataSet(HdfGroup hdfGroup, String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        this.hdfGroup = hdfGroup;
        this.datasetName = datasetName;
        this.hdfDatatype = hdfDatatype;
        this.attributes = new ArrayList<>();
        createInitialMessages(dataSpaceMessage);
    }

    public HdfDataSet(HdfGroup hdfGroup, String datasetName, HdfDatatype hdfDatatype, HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix) {
        this.hdfGroup = hdfGroup;
        this.datasetName = datasetName;
        this.hdfDatatype = hdfDatatype;
        this.attributes = new ArrayList<>();
        this.dataObjectHeaderPrefix = dataObjectHeaderPrefix;
        dataObjectHeaderPrefix.findMessageByType(AttributeMessage.class).ifPresent(attributes::add);
    }

    private void createInitialMessages(DataspaceMessage dataSpaceMessage) {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
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
        FillValueMessage fillValueMessage = new FillValueMessage(2, 2, 2, 1,
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
        if ( objectHeaderSize > headerSize) {
            // headerSize = hdfGroup.getHdfFile().getBufferAllocation().expandDataGroupStorageSize(objectHeaderSize);
            fileAllocation.increaseHeaderAllocation(datasetName, objectHeaderSize);
            // update
            allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        }
        // redo addresses already set.
        // dataLayoutMessage.setDataAddress(HdfFixedPoint.of(hdfGroup.getHdfFile().getBufferAllocation().getDataAddress()));
        dataLayoutMessage.setDataAddress(HdfFixedPoint.of(allocationInfo.getDataOffset()));
        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, objectReferenceCount, Math.max(objectHeaderSize, headerSize), headerMessages);
//        hdfGroup.getHdfFile().recomputeGlobalHeapAddress(this);
    }

    public AttributeMessage createAttribute(String name, DatatypeMessage dt, DataspaceMessage ds, HdfString value) {
        byte[] nameBytes = new byte[name.length()];
        System.arraycopy(name.getBytes(StandardCharsets.US_ASCII), 0, nameBytes, 0, name.length());
        short attributeMessageSize = 8;
        int nameSize = name.toString().length();
        nameSize = (short) ((nameSize + 7) & ~7);
        int datatypeSize = 8; // datatypeMessage.getSizeMessageData();
        int dataspaceSize = 8; // dataspaceMessage.getSizeMessageData();
        int valueSize = value != null ? value.toString().length() : 0;
        valueSize  = (short) ((valueSize + 7) & ~7);
        attributeMessageSize += nameSize + datatypeSize + dataspaceSize + valueSize;
        AttributeMessage attributeMessage = new AttributeMessage(1,
                new HdfString(nameBytes, new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII), name.length()+1)),
                dt, ds, value, (byte)0, attributeMessageSize);
        attributes.add(attributeMessage);
        updateForAttribute();
        return attributeMessage;
    }

    /**
     * See if attribute fits in current objectHeader storage and if not
     * indicate continutationMessage requirements
     */
    private void updateForAttribute() {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        //  int headerSize = hdfGroup.getHdfFile().getBufferAllocation().getDataGroupStorageSize();
        long headerSize = allocationInfo.getHeaderSize();
        List<HdfMessage> headerMessages = this.dataObjectHeaderPrefix.getHeaderMessages();
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        int attributeSize = getAttributeSize();

        checkContinuationMessageNeeded(objectHeaderSize, attributeSize, headerMessages, headerSize);

    }

    @Override
    public void close() {
        if (hdfGroup.getHdfFile() == null) return;
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        // int headerSize = hdfGroup.getHdfFile().getBufferAllocation().getDataGroupStorageSize();
        long headerSize = allocationInfo.getHeaderSize();
        List<HdfMessage> headerMessages = this.dataObjectHeaderPrefix.getHeaderMessages();
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        int attributeSize = getAttributeSize();

        if ( checkContinuationMessageNeeded(objectHeaderSize, attributeSize, headerMessages, headerSize)) {
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
//            int continueSize = dataSpaceMessage.getSizeMessageData() + 8 + attributeSize;
            headerMessages.clear();
            headerMessages.addAll(updatedHeaderMessages);

            objectHeaderContinuationMessage.setContinuationOffset(HdfFixedPoint.of(allocationInfo.getContinuationOffset()));
            objectHeaderContinuationMessage.setContinuationSize(HdfFixedPoint.of(allocationInfo.getContinuationSize()));
            // set the object header size.
        } else if ( objectHeaderSize + attributeSize  < headerSize ) {
            if ( !attributes.isEmpty() ) {
                headerMessages.addAll(attributes);
            }
            long nilSize = headerSize - (objectHeaderSize + attributeSize) - 8;
            headerMessages.add(new NilMessage((int) nilSize, (byte)0, (short)nilSize));
        }
        DataLayoutMessage dataLayoutMessage = dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class).orElseThrow();
        // redo addresses already set.
        // dataLayoutMessage.setDataAddress(HdfFixedPoint.of(hdfGroup.getHdfFile().getBufferAllocation().getDataAddress()));
        dataLayoutMessage.setDataAddress(HdfFixedPoint.of(allocationInfo.getDataOffset()));
//        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, headerMessages.size(), objectReferenceCount, Math.max(objectHeaderSize, headerSize), headerMessages);
//        hdfGroup.getHdfFile().recomputeGlobalHeapAddress(this);
    }

    private boolean checkContinuationMessageNeeded(int objectHeaderSize, int attributeSize, List<HdfMessage> headerMessages, long currentObjectHeaderSize) {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        if ( objectHeaderSize + attributeSize > currentObjectHeaderSize) {
            HdfMessage dataspaceMessage = headerMessages.get(0);
            if ( !(dataspaceMessage instanceof DataspaceMessage)) {
                throw new IllegalArgumentException("Dataspace message not found: " + dataspaceMessage.getClass().getName());
            }
            // hdfGroup.getHdfFile().getBufferAllocation().setDataGroupAndContinuationStorageSize(currentObjectHeaderSize, dataspaceMessage.getSizeMessageData() + 8 + attributeSize);
            // New code:
            long newStorageSize = currentObjectHeaderSize;
            int newContinuationSize = dataspaceMessage.getSizeMessageData() + 8 + attributeSize; // Calculate size first for clarity
            fileAllocation.allocateAndSetDataBlock(datasetName, newStorageSize);
            fileAllocation.allocateAndSetContinuationBlock(datasetName, newContinuationSize);
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

    public void write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        hdfGroup.write(bufferSupplier, this);
    }

    public void write(ByteBuffer byteBuffer) throws IOException {
        hdfGroup.write(byteBuffer, this);
    }

    public HdfFixedPoint getDataAddress() {
        return dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class).orElseThrow().getDataAddress();
    }

    public void writeToFileChannel(FileChannel fileChannel) throws IOException {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        ByteBuffer buffer = ByteBuffer.allocate((int)allocationInfo.getHeaderSize()).order(ByteOrder.LITTLE_ENDIAN);
        dataObjectHeaderPrefix.writeInitialMessageBlockToBuffer(buffer);
        buffer.flip();

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
