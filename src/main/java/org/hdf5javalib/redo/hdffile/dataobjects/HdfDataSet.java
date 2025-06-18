package org.hdf5javalib.redo.hdffile.dataobjects;

import org.hdf5javalib.redo.dataclass.HdfData;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.dataclass.HdfString;
import org.hdf5javalib.redo.dataclass.HdfVariableLength;
import org.hdf5javalib.redo.datatype.*;
import org.hdf5javalib.redo.hdffile.AllocationRecord;
import org.hdf5javalib.redo.hdffile.AllocationType;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.HdfFileAllocation;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.*;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfBTreeV1;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

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
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.hdf5javalib.redo.hdffile.dataobjects.messages.HdfMessage.HDF_MESSAGE_PREAMBLE_SIZE;

/**
 * Represents an HDF5 dataset within an HDF5 file.
 * <p>
 * The {@code HdfDataSet} class manages a dataset, including its name, datatype, attributes,
 * and data storage. It supports creating datasets, adding attributes, writing data, and
 * handling object header messages. Datasets can be scalar, vector, or multi-dimensional,
 * and they may require global heap storage for certain datatypes (e.g., variable-length strings).
 * This class implements {@link Closeable} to ensure proper resource management.
 * </p>
 */
public class HdfDataSet implements HdfDataObject, Closeable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HdfDataSet.class);
    /**
     * The HDF5 file context.
     */
    private final HdfDataFile hdfDataFile;
    /**
     * The name of the dataset.
     */
    private final String datasetName;
    /**
     * The datatype of the dataset.
     */
    private final HdfDatatype hdfDatatype;
    /**
     * The list of attributes associated with the dataset.
     */
    private final List<AttributeMessage> attributes;
    /**
     * The object header prefix for the dataset.
     */
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;
    /**
     * Indicates whether the dataset is closed.
     */
    private boolean closed;

    @Override
    public String toString() {
        Optional<HdfFixedPoint> dataAddress = getDataAddress();
        return "\r\nHdfDataSet@" + (dataAddress.isEmpty() ? "<Empty>" : dataAddress.get().isUndefined() ? "<Undefined>" : dataAddress.get().toString()) + "{" +
                ",\r\n\tdatasetName='" + datasetName + '\'' +
                ",\r\n\thdfDatatype=" + hdfDatatype +
//                ",\r\n\tattributes=" + attributes +
                ",\r\n\tdataObjectHeaderPrefix=" + dataObjectHeaderPrefix +
                ",\r\n\tclosed=" + closed +
                "\r\n\t}";
    }

    /**
     * Constructs an HdfDataSet for creating a new dataset.
     *
     * @param hdfDataFile      the HDF5 file context
     * @param datasetName      the name of the dataset
     * @param hdfDatatype      the datatype of the dataset
     * @param dataSpaceMessage the dataspace message defining the dataset's dimensions
     */
    public HdfDataSet(HdfDataFile hdfDataFile, String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        this.hdfDataFile = hdfDataFile;
        this.datasetName = datasetName;
        this.hdfDatatype = hdfDatatype;
        this.attributes = new ArrayList<>();
        closed = false;
        createInitialMessages(dataSpaceMessage, hdfDatatype);
    }

    /**
     * Constructs an HdfDataSet for an existing dataset.
     *
     * @param hdfDataFile            the HDF5 file context
     * @param datasetName            the name of the dataset
     * @param dataObjectHeaderPrefix the object header prefix for the dataset
     */
    public HdfDataSet(HdfDataFile hdfDataFile, String datasetName, HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix) {
        this.hdfDataFile = hdfDataFile;
        this.datasetName = datasetName;
        this.hdfDatatype = dataObjectHeaderPrefix.findMessageByType(DatatypeMessage.class).orElseThrow().getHdfDatatype();
        this.attributes = new ArrayList<>();
        this.dataObjectHeaderPrefix = dataObjectHeaderPrefix;
        closed = false;
        dataObjectHeaderPrefix.findMessageByType(AttributeMessage.class).ifPresent(attributes::add);
    }

    /**
     * Initializes the object header messages for a new dataset.
     *
     * @param dataSpaceMessage the dataspace message defining the dataset's dimensions
     * @param hdfDatatype      the datatype of the dataset
     */
    private void createInitialMessages(DataspaceMessage dataSpaceMessage, HdfDatatype hdfDatatype) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        Map<AllocationType, AllocationRecord> allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);

        long headerSize = allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getSize().getInstance(Long.class);
        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(dataSpaceMessage);

        // Create datatype message
        short dataTypeMessageSize = (short) ((hdfDatatype.getSizeMessageData() + 7) & ~7); // Align to 8-byte boundary
        DatatypeMessage dataTypeMessage = new DatatypeMessage(hdfDatatype, (byte) 1, dataTypeMessageSize);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        int fillValueWriteTime = hdfDatatype.getDatatypeClass() == HdfDatatype.DatatypeClass.COMPOUND ? 0 : 2;
        FillValueMessage fillValueMessage = new FillValueMessage(2, 2, fillValueWriteTime, 1,
                0, new byte[0], (byte) 1, (short) 8);
        headerMessages.add(fillValueMessage);

        // Add DataLayoutMessage (Contiguous Storage)
        HdfFixedPoint[] dimensions = dataSpaceMessage.getDimensions();
        long dimensionSizes = hdfDatatype.getSize();
        for (HdfFixedPoint fixedPoint : dimensions) {
            dimensionSizes *= fixedPoint.getInstance(Long.class);
        }
        HdfFixedPoint[] hdfDimensionSizes = (HdfFixedPoint[]) Array.newInstance(HdfFixedPoint.class, 1);
        hdfDimensionSizes[0] = HdfWriteUtils.hdfFixedPointFromValue(dimensionSizes, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset());

        short dataLayoutMessageSize = (short) (8 + 16); // Contiguous storage
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1,
                HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                hdfDimensionSizes, 0, null, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().undefined(), (byte) 0, dataLayoutMessageSize);
        headerMessages.add(dataLayoutMessage);

        // Add ObjectModificationTime message
        ObjectModificationTimeMessage objectModificationTimeMessage = new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond(), (byte) 0, (short) 8);
        headerMessages.add(objectModificationTimeMessage);

        // Add attributes
        headerMessages.addAll(attributes);

        int objectReferenceCount = 1;
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        HdfFixedPoint objectHeaderSizePlus16 = HdfWriteUtils.hdfFixedPointFromValue(objectHeaderSize + 16, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset());
        if (objectHeaderSize > headerSize - 16) {
            fileAllocation.increaseHeaderAllocation(datasetName, objectHeaderSizePlus16);
            allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
            headerSize = allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getSize().getInstance(Long.class);
        }

        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(
                1,
                objectReferenceCount,
                Math.max(objectHeaderSize, headerSize - 16),
                headerMessages,
                hdfDataFile,
                "",
                allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getOffset()
        );

        // Allocate data block if needed
        if (allocationInfo.get(AllocationType.DATASET_DATA) == null) {
            boolean requiresGlobalHeap = hdfDatatype.requiresGlobalHeap(false);
            if (requiresGlobalHeap && !hdfDataFile.getFileAllocation().hasGlobalHeapAllocation()) {
                hdfDataFile.getFileAllocation().allocateFirstGlobalHeapBlock();
            }
            hdfDataFile.getFileAllocation().allocateAndSetDataBlock(datasetName, hdfDimensionSizes[0]);
        }
    }

    /**
     * Creates an attribute for the dataset.
     *
     * @param name        the name of the attribute
     * @param value       the value of the attribute
     * @param hdfDataFile the HDF5 file context
     * @return the created {@link AttributeMessage}
     */
    public AttributeMessage createAttribute(String name, String value, HdfDataFile hdfDataFile) {
        boolean requiresGlobalHeap = hdfDatatype.requiresGlobalHeap(false);

        // Create datatype and messages
        HdfDatatype attributeType = createAttributeType(requiresGlobalHeap, value, hdfDataFile);
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
                                        StringDatatype.CharacterSet.ASCII), name.length() + 1, hdfDataFile)),
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
//        attributeMessage.setValue(attributeValue);

        return attributeMessage;
    }

    /**
     * Creates the datatype for an attribute.
     *
     * @param requiresGlobalHeap whether the global heap is required
     * @param value              the attribute value
     * @return the created {@link HdfDatatype}
     */
    private HdfDatatype createAttributeType(boolean requiresGlobalHeap, String value, HdfDataFile hdfDataFile) {
        if (requiresGlobalHeap) {
            FixedPointDatatype fixedType = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField(false, false, false, false),
                    1, (short) 0, (short) 8, hdfDataFile
            );
            return new VariableLengthDatatype(
                    VariableLengthDatatype.createClassAndVersion(),
                    VariableLengthDatatype.createClassBitField(VariableLengthDatatype.Type.STRING, VariableLengthDatatype.PaddingType.NULL_TERMINATE,
                            VariableLengthDatatype.CharacterSet.ASCII),
                    (short) 16, fixedType,
                    hdfDataFile
            );
        }
        return new StringDatatype(
                StringDatatype.createClassAndVersion(),
                StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE,
                        StringDatatype.CharacterSet.ASCII),
                (short) value.length(), hdfDataFile
        );
    }

    /**
     * Creates a datatype message for an attribute.
     *
     * @param attributeType the attribute datatype
     * @return the created {@link DatatypeMessage}
     */
    private DatatypeMessage createDatatypeMessage(HdfDatatype attributeType) {
        short dataTypeMessageSize = (short) attributeType.getSizeMessageData();
        return new DatatypeMessage(attributeType, (byte) 1, dataTypeMessageSize);
    }

    /**
     * Creates a dataspace message for an attribute (scalar).
     *
     * @return the created {@link DataspaceMessage}
     */
    private DataspaceMessage createDataspaceMessage() {
        short dataSpaceMessageSize = 8;
        return new DataspaceMessage(
                1, 0, DataspaceMessage.buildFlagSet(false, false),
                null, null, false, (byte) 0, dataSpaceMessageSize
        );
    }

    /**
     * Creates the value for an attribute.
     *
     * @param value              the attribute value
     * @param attributeType      the attribute datatype
     * @param requiresGlobalHeap whether the global heap is required
     * @param hdfDataFile        the HDF5 file context
     * @return the created {@link HdfData} value
     */
    private HdfData createAttributeValue(String value, HdfDatatype attributeType,
                                         boolean requiresGlobalHeap, HdfDataFile hdfDataFile) {
        if (requiresGlobalHeap) {
            VariableLengthDatatype variableLengthType = new VariableLengthDatatype(
                    VariableLengthDatatype.createClassAndVersion(),
                    VariableLengthDatatype.createClassBitField(VariableLengthDatatype.Type.STRING, VariableLengthDatatype.PaddingType.NULL_TERMINATE,
                            VariableLengthDatatype.CharacterSet.ASCII),
                    (short) 16, attributeType,
                    hdfDataFile
            );
            variableLengthType.setGlobalHeap(hdfDataFile.getGlobalHeap());
            byte[] globalHeapBytes = hdfDataFile.getGlobalHeap().addToHeap(
                    value.getBytes(StandardCharsets.US_ASCII));
            return new HdfVariableLength(globalHeapBytes, variableLengthType);
        }
        return new HdfString(value.getBytes(StandardCharsets.US_ASCII), (StringDatatype) attributeType);
    }

    /**
     * Aligns a size to an 8-byte boundary.
     *
     * @param size the size to align
     * @return the aligned size
     */
    private short alignTo8Bytes(int size) {
        return (short) ((size + 7) & ~7);
    }

    /**
     * Calculates the size of an attribute value with alignment.
     *
     * @param value              the attribute value
     * @param requiresGlobalHeap whether the global heap is required
     * @return the aligned size in bytes
     */
    private int calculateValueSize(String value, boolean requiresGlobalHeap) {
        if (requiresGlobalHeap) {
            return 16; // Fixed size for global heap
        }
        int length = (value != null) ? value.length() : 0;
        return alignTo8Bytes(length);
    }

    /**
     * Updates the object header to accommodate a new attribute.
     */
    private void updateForAttribute() {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        Map<AllocationType, AllocationRecord> allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        long headerSize = allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getSize().getInstance(Long.class);
        List<HdfMessage> headerMessages = this.dataObjectHeaderPrefix.getHeaderMessages();
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        int attributeSize = getAttributeSize();

        checkContinuationMessageNeeded(objectHeaderSize, attributeSize, headerMessages, headerSize - 16);
    }

    /**
     * Closes the dataset, finalizing its state and writing to the file.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (closed) return;
        if (hdfDataFile.getFileAllocation().getDatasetAllocationInfo(datasetName).isEmpty()) return;
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        Map<AllocationType, AllocationRecord> allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        long headerSize = allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getSize().getInstance(Long.class);
        List<HdfMessage> headerMessages = this.dataObjectHeaderPrefix.getHeaderMessages();
        int objectHeaderSize = getObjectHeaderSize(headerMessages);
        int attributeSize = getAttributeSize();

        if (checkContinuationMessageNeeded(objectHeaderSize, attributeSize, headerMessages, headerSize - 16)) {
            allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
            List<HdfMessage> updatedHeaderMessages = new ArrayList<>();

            ObjectHeaderContinuationMessage objectHeaderContinuationMessage = new ObjectHeaderContinuationMessage(
                    HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset()),
                    HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength()),
                    (byte) 0, (short) 16);
            updatedHeaderMessages.add(objectHeaderContinuationMessage);
            updatedHeaderMessages.add(new NilMessage((byte) 0, (short) 0));
            HdfMessage dataSpaceMessage = headerMessages.remove(0);
            if (!(dataSpaceMessage instanceof DataspaceMessage)) {
                throw new IllegalStateException("Find DataspaceMessage for " + datasetName);
            }
            updatedHeaderMessages.addAll(headerMessages);

            updatedHeaderMessages.add(dataSpaceMessage);
            updatedHeaderMessages.addAll(attributes);
            attributes.clear();
            headerMessages.clear();
            headerMessages.addAll(updatedHeaderMessages);

            objectHeaderContinuationMessage.setContinuationOffset(allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION).getOffset());
            objectHeaderContinuationMessage.setContinuationSize(allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION).getSize());
        } else if (objectHeaderSize + attributeSize < headerSize - 16) {
            if (!attributes.isEmpty()) {
                headerMessages.addAll(attributes);
                attributes.clear();
            }
            long nilSize = (headerSize - 16) - (objectHeaderSize + attributeSize) - 8;
            headerMessages.add(new NilMessage((byte) 0, (short) nilSize));
        }
        DataLayoutMessage dataLayoutMessage = dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class).orElseThrow();
        dataLayoutMessage.setDataAddress(allocationInfo.get(AllocationType.DATASET_DATA).getOffset());
        writeToFileChannel(hdfDataFile.getSeekableByteChannel());

        closed = true;
    }

    /**
     * Checks if a continuation message is needed for the object header.
     *
     * @param objectHeaderSize the size of the object header messages
     * @param attributeSize    the size of the attributes
     * @param headerMessages   the list of header messages
     * @param headerSize       the allocated header size
     * @return true if a continuation message is needed, false otherwise
     */
    private boolean checkContinuationMessageNeeded(int objectHeaderSize, int attributeSize, List<HdfMessage> headerMessages, long headerSize) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        Map<AllocationType, AllocationRecord> allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);

        if (objectHeaderSize + attributeSize > headerSize
                && (allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION) == null || !attributes.isEmpty())) {
            HdfMessage dataspaceMessage = headerMessages.get(0);
            if (!(dataspaceMessage instanceof DataspaceMessage)) {
                throw new IllegalArgumentException("Dataspace message not found: " + dataspaceMessage.getClass().getName());
            }
            if (allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getSize().getInstance(Long.class) < headerSize) {
                fileAllocation.increaseHeaderAllocation(datasetName, HdfWriteUtils.hdfFixedPointFromValue(headerSize, fileAllocation.getSuperblock().getFixedPointDatatypeForLength()));
            }
            int newContinuationSize = dataspaceMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE + attributeSize;
            if (allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION) == null || (allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION).getSize().getInstance(Long.class) < newContinuationSize)) {
                fileAllocation.allocateAndSetContinuationBlock(datasetName,
                        HdfWriteUtils.hdfFixedPointFromValue(newContinuationSize, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength()))
                ;
            }
            return true;
        }
        return false;
    }

    /**
     * Calculates the total size of the attributes.
     *
     * @return the size of the attributes in bytes
     */
    private int getAttributeSize() {
        int attributeSize = 0;
        for (HdfMessage hdfMessage : attributes) {
            log.trace("Write: hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE = {} {}", hdfMessage.getMessageType(), hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE);
            attributeSize += hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE;
        }
        return attributeSize;
    }

    /**
     * Calculates the total size of the object header messages.
     *
     * @param headerMessages the list of header messages
     * @return the size of the header messages in bytes
     */
    private int getObjectHeaderSize(List<HdfMessage> headerMessages) {
        int objectHeaderSize = 0;
        for (HdfMessage hdfMessage : headerMessages) {
            log.trace("Write: hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE = {} {}", hdfMessage.getMessageType(), hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE);
            objectHeaderSize += hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE;
        }
        return objectHeaderSize;
    }

    /**
     * Writes data to the dataset using a buffer supplier.
     *
     * @param bufferSupplier the supplier providing ByteBuffer instances
     * @throws IOException if an I/O error occurs
     */
    @SuppressWarnings("resource")
    public void write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        Map<AllocationType, AllocationRecord> allocationInfo = hdfDataFile.getFileAllocation().getDatasetAllocationInfo(datasetName);
        hdfDataFile.getSeekableByteChannel().position(allocationInfo.get(AllocationType.DATASET_DATA).getOffset().getInstance(Long.class));
        ByteBuffer buffer;
        while ((buffer = bufferSupplier.get()).hasRemaining()) {
            while (buffer.hasRemaining()) {
                hdfDataFile.getSeekableByteChannel().write(buffer);
            }
        }
    }

    /**
     * Writes data to the dataset from a single ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the data
     * @throws IOException if an I/O error occurs
     */
    @SuppressWarnings("resource")
    public void write(ByteBuffer buffer) throws IOException {
        Map<AllocationType, AllocationRecord> allocationInfo = hdfDataFile.getFileAllocation().getDatasetAllocationInfo(datasetName);
        hdfDataFile.getSeekableByteChannel().position(allocationInfo.get(AllocationType.DATASET_DATA).getOffset().getInstance(Long.class));
        while (buffer.hasRemaining()) {
            hdfDataFile.getSeekableByteChannel().write(buffer);
        }
    }

    /**
     * Retrieves the data address of the dataset.
     *
     * @return the {@link HdfFixedPoint} representing the data address
     */
    public Optional<HdfFixedPoint> getDataAddress() {
        return dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class)
                .flatMap(dataLayoutMessage -> Optional.ofNullable(dataLayoutMessage.getDataAddress()));
    }

    /**
     * Retrieves the dimension sizes of the dataset.
     *
     * @return an array of {@link HdfFixedPoint} representing the dimension sizes
     */
    public Optional<HdfFixedPoint[]> getdimensionSizes() {
        return dataObjectHeaderPrefix.findMessageByType(DataLayoutMessage.class)
                .flatMap(dataLayoutMessage -> Optional.ofNullable(dataLayoutMessage.getDimensionSizes()));
    }

    /**
     * Writes the dataset's object header to the file channel.
     *
     * @param fileChannel the file channel to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        Map<AllocationType, AllocationRecord> allocationInfo = fileAllocation.getDatasetAllocationInfo(datasetName);
        ByteBuffer buffer = ByteBuffer.allocate(allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getSize().getInstance(Integer.class)).order(ByteOrder.LITTLE_ENDIAN);
        dataObjectHeaderPrefix.writeInitialMessageBlockToBuffer(buffer);
        buffer.rewind();

        fileChannel.position(allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getOffset().getInstance(Long.class));
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        if (allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION) != null) {
            buffer = ByteBuffer.allocate(allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION).getSize().getInstance(Integer.class)).order(ByteOrder.LITTLE_ENDIAN);
            dataObjectHeaderPrefix.writeContinuationMessageBlockToBuffer(allocationInfo.get(AllocationType.DATASET_OBJECT_HEADER).getSize().getInstance(Integer.class), buffer);
            buffer.flip();

            fileChannel.position(allocationInfo.get(AllocationType.DATASET_HEADER_CONTINUATION).getOffset().getInstance(Long.class));
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        }
    }

    public HdfDatatype getHdfDatatype() {
        return hdfDatatype;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public HdfObjectHeaderPrefixV1 getDataObjectHeaderPrefix() {
        return dataObjectHeaderPrefix;
    }

    @Override
    public String getObjectName() {
        return datasetName;
    }

    public Optional<HdfBTreeV1> getBTreeOptionally() {
        return Optional.empty();
    }

    public int getDimensionality() {
        return dataObjectHeaderPrefix.findMessageByType(DataspaceMessage.class).orElseThrow().getDimensionality();
    }

    public boolean hasDataspaceMessage() {
        return dataObjectHeaderPrefix.findMessageByType(DataspaceMessage.class).isPresent();
    }

    public List<AttributeMessage> getAttributeMessages() {
        return attributes;
    }

    public List<ReferenceDatatype> getReferenceInstances() {
        DatatypeMessage dt = dataObjectHeaderPrefix.findMessageByType(DatatypeMessage.class).orElseThrow();
        return dt.getHdfDatatype().getReferenceInstances();
    }
}