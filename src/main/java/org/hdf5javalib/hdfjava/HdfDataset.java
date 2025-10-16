package org.hdf5javalib.hdfjava;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datatype.Datatype;
import org.hdf5javalib.datatype.ReferenceDatatype;
import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.hdffile.dataobjects.messages.*;
import org.hdf5javalib.hdffile.infrastructure.HdfBTreeV1ForChunk;
import org.hdf5javalib.hdffile.infrastructure.HdfGroupForChunkBTreeEntry;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

/**
 * Represents a leaf node (HdfDataset) in the B-Tree.
 */
public class HdfDataset extends HdfDataObject implements AutoCloseable {

    public HdfDataset(String objectName, HdfObjectHeaderPrefix objectHeader, HdfTreeNode parent, String hardLink) {
        super(objectName, objectHeader, parent, hardLink);
    }

    @Override
    public int getLevel() {
        return 0;
    }

    @Override
    public boolean isDataset() {
        return true;
    }

    @Override
    public void close() throws Exception {
        // not implemented
    }

    public void createAttribute(String attributeName, String attributeValue, HdfDataFile hdfDataFile) {
        // not implemented
    }

    public Datatype getDatatype() {
        return objectHeader.findMessageByType(DatatypeMessage.class).orElseThrow().getHdfDatatype();
    }

//    /**
//     * Retrieves the dimension sizes of the dataset.
//     *
//     * @return an array of {@link HdfFixedPoint} representing the dimension sizes
//     */
//    public HdfFixedPoint[] getDimensionSizes() {
//        return objectHeader.findMessageByType(DataLayoutMessage.class).orElseThrow().getDimensionSizes();
//    }

    @Override
    public String getObjectName() {
        return objectName;
    }

    public int getDimensionality() {
        return objectHeader.findMessageByType(DataspaceMessage.class).orElseThrow().getDimensionality();
    }

    public boolean hasDataspaceMessage() {
        return objectHeader.findMessageByType(DataspaceMessage.class).isPresent();
    }

    public List<AttributeMessage> getAttributeMessages() {
        return objectHeader.getHeaderMessages().stream()
                .filter(m -> m.getMessageType() == HdfMessage.MessageType.ATTRIBUTE_MESSAGE)
                .map(m -> (AttributeMessage) m)
                .toList();
    }

    public List<ReferenceDatatype> getReferenceInstances() {
        DatatypeMessage dt = objectHeader.findMessageByType(DatatypeMessage.class).orElseThrow();
        return dt.getHdfDatatype().getReferenceInstances();
    }
    /**
     * Extracts the dimensions from a DataspaceMessage.
     *
     * @return an array of dimension sizes
     */
    public int[] extractDimensions() throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        Optional<DataspaceMessage> optDSM = objectHeader.findMessageByType(DataspaceMessage.class);
        if ( optDSM.isEmpty() ) {
            return new int[0];
        }
        HdfFixedPoint[] dims = optDSM.get().getDimensions();
        int[] result = new int[dims.length];
        for (int i = 0; i < dims.length; i++) {
            result[i] = dims[i].getInstance(Long.class).intValue();
        }
        return result;
    }

    public int getElementSize() {
        return getDatatype().getSize();
    }

//    public synchronized ByteBuffer getDatasetData(SeekableByteChannel channel, long offset, long size) throws IOException {
//        DataLayoutMessage.DataLayoutStorage dataLayoutStorage = objectHeader.findMessageByType(DataLayoutMessage.class).orElseThrow().getDataLayoutStorage();
//
//        // I need code here for three types of DataLayoutMessage.DataLayoutStorage implementation.
//        // Also need to define what methods will be needed in DataLayoutMessage.DataLayoutStorage abstract class.
//
//        // First implementation of DataLayoutMessage.DataLayoutStorage has two properties. These are:
//        // private final int compactDataSize;
//        // private final byte[] compactData;
//        // it is just the data available. Therefore, channel won't be needed, offset and size should add up to compactDataSize,
//        // and whatever is requested via the offset and size parameter values should be delivered in ByteBuffer as shown below.
//
//        // Second implementation of DataLayoutMessage.DataLayoutStorage has two properties. These are:
//        // private final HdfFixedPoint dataAddress;
//        // private final HdfFixedPoint dataSize;
//        // Again, these are the data available, but the data is still on disk and not in memory as in the first implementation.
//        // Therefore, the channel will need to read what is requested by the offset and size parameters.
//        // An HdfFixedPoint value be be converted to a Long value with, for example, dataAddress.getInstance(Long.class), or a
//        // int value with dataAddress.getInstance(Long.class).intValue(). Again, whatever is requested via the offset and size parameter values should be delivered in ByteBuffer as shown below.
//
//        // The third implementation is complicated and should have stubbed code for now, returning an empty ByteBuffer.
//
//        ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
//        channel.position(dataAddress.getInstance(Long.class) + offset);
//        int bytesRead = channel.read(buffer);
//        if (bytesRead != size) {
//            throw new IOException("Failed to read the expected number of bytes: read " + bytesRead + ", expected " + size);
//        }
//        buffer.flip();
//        return buffer;
//    }

    // Assuming necessary imports for HDF5 classes

    // Helper method (place this as a static method in your class)
    private static long computeFlattenedIndex(long[] indices, long[] dims) {
        int rank = indices.length;
        long index = 0;
        long stride = 1;
        for (int d = rank - 1; d >= 0; d--) {
            index += indices[d] * stride;
            stride *= dims[d];
        }
        return index;
    }

    /**
     * A private record to hold the context and pre-calculated values for a chunked data read.
     * This avoids passing numerous parameters to helper methods.
     */
    private record ChunkedReadContext(
            int dimensions,
            long[] datasetDims,
            long[] chunkDims,
            long elementSize,
            long startElement,
            long endElement,
            Optional<FilterPipelineMessage> filterPipeline,
            FillValueMessage fillValueMessage
    ) {
        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            ChunkedReadContext that = (ChunkedReadContext) o;
            return dimensions == that.dimensions && endElement == that.endElement && elementSize == that.elementSize && startElement == that.startElement && Objects.deepEquals(chunkDims, that.chunkDims) && Objects.deepEquals(datasetDims, that.datasetDims);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dimensions, Arrays.hashCode(datasetDims), Arrays.hashCode(chunkDims), elementSize, startElement, endElement);
        }

        @Override
        public String toString() {
            return "ChunkedReadContext{" +
                    "dimensions=" + dimensions +
                    ", datasetDims=" + Arrays.toString(datasetDims) +
                    ", chunkDims=" + Arrays.toString(chunkDims) +
                    ", elementSize=" + elementSize +
                    ", startElement=" + startElement +
                    ", endElement=" + endElement +
                    '}';
        }
    }

    // In the method:
    public synchronized ByteBuffer getDatasetData(SeekableByteChannel channel, long offset, long size) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        DataLayoutMessage.DataLayoutStorage dataLayoutStorage = objectHeader.findMessageByType(DataLayoutMessage.class)
                .orElseThrow(() -> new IOException("DataLayoutMessage not found"))
                .getDataLayoutStorage();

        if (dataLayoutStorage instanceof DataLayoutMessage.CompactStorage compact) {
            return readCompactData(compact, offset, size);
        } else if (dataLayoutStorage instanceof DataLayoutMessage.ContiguousStorage contiguous) {
            return readContiguousData(contiguous, channel, offset, size);
        } else if (dataLayoutStorage instanceof DataLayoutMessage.ChunkedStorage chunked) {
            return readChunkedData(chunked, channel, offset, size);
        } else {
            throw new UnsupportedOperationException("Unsupported DataLayoutStorage type: " + dataLayoutStorage.getClass().getName());
        }
    }

    private ByteBuffer readCompactData(DataLayoutMessage.CompactStorage compact, long offset, long size) {
        if (offset < 0 || size < 0 || offset + size > compact.getCompactDataSize()) {
            throw new IllegalArgumentException("Invalid offset or size for compact data");
        }
        // Use wrap to create a view into the existing array without copying
        return ByteBuffer.wrap(compact.getCompactData(), (int) offset, (int) size).order(ByteOrder.LITTLE_ENDIAN);
    }

    private ByteBuffer readContiguousData(DataLayoutMessage.ContiguousStorage contiguous, SeekableByteChannel channel, long offset, long size) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        long addr = contiguous.getDataAddress().getInstance(Long.class);
        long totalSize = contiguous.getDataSize().getInstance(Long.class);
        if (offset < 0 || size < 0 || offset + size > totalSize) {
            throw new IllegalArgumentException("Invalid offset or size for contiguous data");
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
        channel.position(addr + offset);
        int bytesRead = channel.read(buffer);
        if (bytesRead != size) {
            throw new IOException("Failed to read the expected number of bytes: read " + bytesRead + ", expected " + size);
        }
        buffer.flip();
        return buffer;
    }

    private ByteBuffer readChunkedData(DataLayoutMessage.ChunkedStorage chunked, SeekableByteChannel channel, long offset, long size) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ChunkedReadContext context = setupChunkedReadContext(chunked, offset, size);
        ByteBuffer buffer = createAndFillResultBuffer(size, context.fillValueMessage(), context.elementSize());

        List<HdfGroupForChunkBTreeEntry> chunkEntries = new ArrayList<>();
        collectLeafEntries(chunked.getBTree(), chunkEntries);

        for (HdfGroupForChunkBTreeEntry entry : chunkEntries) {
            processSingleChunk(entry, channel, buffer, context);
        }

        buffer.flip();
        return buffer;
    }

    private ChunkedReadContext setupChunkedReadContext(DataLayoutMessage.ChunkedStorage chunked, long offset, long size) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        DataspaceMessage dataspace = objectHeader.findMessageByType(DataspaceMessage.class).orElseThrow(() -> new IOException("DataspaceMessage not found"));
        int dimensions = dataspace.getDimensionality();

        long[] datasetDims = new long[dimensions];
        HdfFixedPoint[] datasetDimsHdf = dataspace.getDimensions();
        for (int i = 0; i < dimensions; i++) {
            datasetDims[i] = datasetDimsHdf[i].getInstance(Long.class);
        }

        long[] chunkDims = new long[dimensions];
        for (int i = 0; i < dimensions; i++) {
            chunkDims[i] = chunked.getDimensionSizes()[i].getInstance(Long.class);
        }

        long elementSize = chunked.getDatasetElementSize().getInstance(Long.class);
        long totalElements = 1;
        for (long dim : datasetDims) {
            totalElements *= dim;
        }

        long totalSize = totalElements * elementSize;
        if (offset < 0 || size < 0 || offset + size > totalSize) {
            throw new IllegalArgumentException("Invalid offset or size for chunked data");
        }
        if (offset % elementSize != 0 || size % elementSize != 0) {
            throw new IllegalArgumentException("Offset and size must be multiples of element size");
        }

        long startElement = offset / elementSize;
        long numElements = size / elementSize;
        long endElement = startElement + numElements - 1;

        FillValueMessage fillMsg = objectHeader.findMessageByType(FillValueMessage.class).orElse(null);
        Optional<FilterPipelineMessage> fpm = objectHeader.findMessageByType(FilterPipelineMessage.class);

        return new ChunkedReadContext(dimensions, datasetDims, chunkDims, elementSize, startElement, endElement, fpm, fillMsg);
    }

    private ByteBuffer createAndFillResultBuffer(long size, FillValueMessage fillMsg, long elementSize) {
        ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
        if (fillMsg == null || fillMsg.getFillValueDefined() != 1 || fillMsg.getSize() == 0) {
            return buffer; // Defaults to zero-filled, which is the common case
        }

        int patternLength = fillMsg.getSize();
        byte[] fillPattern = fillMsg.getFillValue();

        if (patternLength > 0) {
            if (patternLength != elementSize) {
                throw new UnsupportedOperationException("Fill pattern size != element size; complex types not handled yet");
            }
            for (int p = 0; p < buffer.capacity(); p += patternLength) {
                buffer.put(fillPattern);
                buffer.position(p + patternLength);
            }
            buffer.position(0); // Reset position after filling
        }
        return buffer;
    }

    private void processSingleChunk(HdfGroupForChunkBTreeEntry entry, SeekableByteChannel channel, ByteBuffer resultBuffer, ChunkedReadContext context) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        long[] chunkOffset = new long[context.dimensions()];
        for (int i = 0; i < context.dimensions(); i++) {
            chunkOffset[i] = entry.getDimensionOffsets().get(i).getInstance(Long.class);
        }

        long[] chunkActualSize = new long[context.dimensions()];
        for (int i = 0; i < context.dimensions(); i++) {
            chunkActualSize[i] = Math.min(context.chunkDims()[i], context.datasetDims()[i] - chunkOffset[i]);
        }

        // Quick overlap test
        long chunkMinElement = computeFlattenedIndex(chunkOffset, context.datasetDims());
        long[] maxIdx = new long[context.dimensions()];
        for (int i = 0; i < context.dimensions(); i++) {
            maxIdx[i] = chunkOffset[i] + chunkActualSize[i] - 1;
        }
        long chunkMaxElement = computeFlattenedIndex(maxIdx, context.datasetDims());

        if (chunkMaxElement < context.startElement() || chunkMinElement > context.endElement()) {
            return; // No overlap with this chunk
        }

        ByteBuffer chunkBuffer = readAndDecodeChunk(entry, channel, context);
        copyDataFromChunk(chunkBuffer, resultBuffer, chunkOffset, chunkActualSize, context);
    }

    private ByteBuffer readAndDecodeChunk(HdfGroupForChunkBTreeEntry entry, SeekableByteChannel channel, ChunkedReadContext context) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        long addr = entry.getChildPointer().getInstance(Long.class);
        long sizeOnDisk = entry.getSizeOfChunk();
        ByteBuffer chunkBuffer = ByteBuffer.allocate((int) sizeOnDisk).order(ByteOrder.LITTLE_ENDIAN);
        channel.position(addr);
        int bytesRead = channel.read(chunkBuffer);
        if (bytesRead != sizeOnDisk) {
            throw new IOException("Failed to read chunk: read " + bytesRead + ", expected " + sizeOnDisk);
        }
        chunkBuffer.flip();

        if (context.filterPipeline().isPresent()) {
            chunkBuffer = context.filterPipeline().get().getDeflater().deflate(chunkBuffer);
        }
        if (entry.getFilterMask() != 0) {
            throw new UnsupportedOperationException("Filters not supported yet");
        }

        long expectedChunkSize = 1;
        for (long s : context.chunkDims()) {
            expectedChunkSize *= s;
        }
        expectedChunkSize *= context.elementSize();
        if (chunkBuffer.remaining() != expectedChunkSize) {
            throw new IOException("Chunk size mismatch: got " + chunkBuffer.remaining() + ", expected " + expectedChunkSize);
        }
        return chunkBuffer;
    }

    private void copyDataFromChunk(ByteBuffer chunkBuffer, ByteBuffer resultBuffer, long[] chunkOffset, long[] chunkActualSize, ChunkedReadContext context) {
        long[] localIdx = new long[context.dimensions()];
        boolean done = false;
        while (!done) {
            long[] globalIdx = new long[context.dimensions()];
            for (int i = 0; i < context.dimensions(); i++) {
                globalIdx[i] = chunkOffset[i] + localIdx[i];
            }
            long globalElement = computeFlattenedIndex(globalIdx, context.datasetDims());

            if (globalElement >= context.startElement() && globalElement <= context.endElement()) {
                long localElement = computeFlattenedIndex(localIdx, context.chunkDims());
                long sourcePos = localElement * context.elementSize();
                long destPos = (globalElement - context.startElement()) * context.elementSize();

                chunkBuffer.position((int) sourcePos);
                resultBuffer.position((int) destPos);
                for (int b = 0; b < context.elementSize(); b++) {
                    resultBuffer.put(chunkBuffer.get());
                }
            }

            // Increment local index (odometer)
            int pos = context.dimensions() - 1;
            while (pos >= 0) {
                localIdx[pos]++;
                if (localIdx[pos] < chunkActualSize[pos]) {
                    break;
                }
                localIdx[pos] = 0;
                pos--;
            }
            if (pos < 0) {
                done = true;
            }
        }
    }


    private void collectLeafEntries(HdfBTreeV1ForChunk bTree, List<HdfGroupForChunkBTreeEntry> leafEntries) {
        if (bTree == null) {
            return;
        }

        // If this is a leaf node (nodeLevel == 0), add its entries
        if (bTree.getNodeLevel() == 0) {
            List<HdfGroupForChunkBTreeEntry> entries = bTree.getEntries();
            if (entries != null) {
                leafEntries.addAll(entries);
            }
        } else {
            // If this is an internal node (nodeLevel > 0), recurse into child B-Trees
            List<HdfGroupForChunkBTreeEntry> entries = bTree.getEntries();
            if (entries != null) {
                for (HdfGroupForChunkBTreeEntry entry : entries) {
                    HdfBTreeV1ForChunk childBTree = entry.getChildBTree();
                    collectLeafEntries(childBTree, leafEntries);
                }
            }
        }
    }

    public boolean hasData() {
        return hardLink == null
                && hasDataspaceMessage()
                && objectHeader.findMessageByType(DatatypeMessage.class).isPresent()
                && objectHeader.findMessageByType(DataLayoutMessage.class).orElseThrow().hasData();
    }

    /**
     * Retrieves the data address of the dataset.
     *
     * @return the {@link org.hdf5javalib.redo.dataclass.HdfFixedPoint} representing the data address
     */
//    private Optional<HdfFixedPoint> getDataAddress() {
//        return objectHeader.findMessageByType(DataLayoutMessage.class)
//                .flatMap(dataLayoutMessage -> Optional.ofNullable(dataLayoutMessage.getDataAddress()));
//    }


}