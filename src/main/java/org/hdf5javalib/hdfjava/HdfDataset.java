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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

    // In the method:
    public synchronized ByteBuffer getDatasetData(SeekableByteChannel channel, long offset, long size) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        DataLayoutMessage.DataLayoutStorage dataLayoutStorage = objectHeader.findMessageByType(DataLayoutMessage.class).orElseThrow().getDataLayoutStorage();

        if (dataLayoutStorage instanceof DataLayoutMessage.CompactStorage compact) {
            if (offset < 0 || size < 0 || offset + size > compact.getCompactDataSize()) {
                throw new IllegalArgumentException("Invalid offset or size for compact data");
            }
            ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put(compact.getCompactData(), (int) offset, (int) size);
            buffer.flip();
            return buffer;
        } else if (dataLayoutStorage instanceof DataLayoutMessage.ContiguousStorage contiguous) {
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
        } else if (dataLayoutStorage instanceof DataLayoutMessage.ChunkedStorage chunked) {
            // The full chunked branch:
            DataspaceMessage dataspace = objectHeader.findMessageByType(DataspaceMessage.class).orElseThrow();
            int dimensions = dataspace.getDimensionality();
            HdfFixedPoint[] datasetDimsHdf = dataspace.getDimensions(); // e.g., [6, 8]
            long[] datasetDims = new long[datasetDimsHdf.length];
            for (int i = 0; i < datasetDims.length; i++) {
                datasetDims[i] = datasetDimsHdf[i].getInstance(Long.class);
            }

            long elementSize = chunked.getDatasetElementSize().getInstance(Long.class); // e.g., 4

            long totalElements = 1;
            for (long dim : datasetDims) {
                totalElements *= dim;
            }
            long totalSize = totalElements * elementSize;
            if (offset < 0 || size < 0 || offset + size > totalSize) {
                throw new IllegalArgumentException("Invalid offset or size for chunked data");
            }

            // Assuming offset and size are aligned to element boundaries
            if (offset % elementSize != 0 || size % elementSize != 0) {
                throw new IllegalArgumentException("Offset and size must be multiples of element size");
            }

            ByteBuffer buffer = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);

            // Pre-fill buffer with fill value (defaults to zero if undefined or size=0)
            FillValueMessage fillMsg = objectHeader.findMessageByType(FillValueMessage.class).orElse(null);
            byte[] fillPattern = null;
            int patternLength = 0;
            if (fillMsg != null && fillMsg.getFillValueDefined() == 1) {  // Assuming getter methods like getFillValueDefined(), getSize(), getFillValue()
                patternLength = fillMsg.getSize();
                fillPattern = fillMsg.getFillValue();
            }
            if (patternLength > 0) {
                if (patternLength != elementSize) {
                    throw new UnsupportedOperationException("Fill pattern size != element size; complex types not handled yet");
                }
                for (int p = 0; p < buffer.capacity(); p += patternLength) {
                    buffer.position(p);
                    buffer.put(fillPattern);
                }
            }  // Else, leave as zero-filled (matches example where size=0 and fill=0)

            long startElement = offset / elementSize;
            long numElements = size / elementSize;
            long endElement = startElement + numElements - 1;

            HdfBTreeV1ForChunk bTree = chunked.getBTree();
            // Assuming simple leaf node with no siblings or internal nodes for this implementation
//            List<HdfGroupForChunkBTreeEntry> chunkEntries = bTree.getEntries().stream().toList();
            List<HdfGroupForChunkBTreeEntry> chunkEntries = new ArrayList<>();
            collectLeafEntries(bTree, chunkEntries);

            long[] chunkDims = new long[dimensions];
            for (int i = 0; i < dimensions; i++) {
                chunkDims[i] = chunked.getDimensionSizes()[i].getInstance(Long.class); // e.g., [4, 4]
            }

            for (HdfGroupForChunkBTreeEntry entry : chunkEntries) {
                List<HdfFixedPoint> offsets = entry.getDimensionOffsets();
                if (offsets.size() != dimensions) {
                    throw new IOException("Invalid dimension offsets size");
                }
                long lastOffset = offsets.get(dimensions-1).getInstance(Long.class);
//                if (lastOffset != 0) {
//                    throw new IOException("Last dimension offset should be zero");
//                }

                long[] chunkOffset = new long[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    chunkOffset[i] = offsets.get(i).getInstance(Long.class);
                }

                long[] chunkActualSize = new long[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    chunkActualSize[i] = Math.min(chunkDims[i], datasetDims[i] - chunkOffset[i]);
                }

                // Compute min and max element for quick overlap test
                long[] minIdx = chunkOffset.clone();
                long chunkMinElement = computeFlattenedIndex(minIdx, datasetDims);

                long[] maxIdx = new long[dimensions];
                for (int i = 0; i < dimensions; i++) {
                    maxIdx[i] = chunkOffset[i] + chunkActualSize[i] - 1;
                }
                long chunkMaxElement = computeFlattenedIndex(maxIdx, datasetDims);

                if (chunkMaxElement < startElement || chunkMinElement > endElement) {
                    continue; // No overlap
                }

                // Read the raw chunk (full size, even if partial)
                long addr = entry.getChildPointer().getInstance(Long.class);
                long sizeOnDisk = entry.getSizeOfChunk();
                ByteBuffer chunkBuffer = ByteBuffer.allocate((int) sizeOnDisk).order(ByteOrder.LITTLE_ENDIAN);
                channel.position(addr);
                int bytesRead = channel.read(chunkBuffer);
                if (bytesRead != sizeOnDisk) {
                    throw new IOException("Failed to read chunk: read " + bytesRead + ", expected " + sizeOnDisk);
                }
                chunkBuffer.flip();

                    Optional<FilterPipelineMessage> fpm = objectHeader.findMessageByType(FilterPipelineMessage.class);
                    if ( fpm.isPresent() ) {
                        chunkBuffer = fpm.get().getDeflater().deflate(chunkBuffer);
                    }
                // Handle filters/decompression (stubbed)
                if (entry.getFilterMask() != 0) {
                    throw new UnsupportedOperationException("Filters  not supported yet");
                }

                // Validate chunk size matches expected full size (uncompressed)
                long expectedChunkSize = 1;
                for (long s : chunkDims) {  // Use chunkDims for full stored size
                    expectedChunkSize *= s;
                }
                expectedChunkSize *= elementSize;
                if (chunkBuffer.remaining() != expectedChunkSize) {
                    throw new IOException("Chunk size mismatch: got " + chunkBuffer.remaining() + ", expected " + expectedChunkSize);
                }

                // Iterate over all valid local indices using odometer
                long[] localIdx = new long[dimensions];
                boolean done = false;
                long[] globalIdx = new long[dimensions];
                while (!done) {
                    // Compute global element index
                    for (int i = 0; i < dimensions; i++) {
                        globalIdx[i] = chunkOffset[i] + localIdx[i];
                    }
                    long globalElement = computeFlattenedIndex(globalIdx, datasetDims);

                    if (globalElement >= startElement && globalElement <= endElement) {
                        // Compute local flat position (using chunkDims strides)
                        long localElement = computeFlattenedIndex(localIdx, chunkDims);
                        long sourcePos = localElement * elementSize;
                        long destPos = (globalElement - startElement) * elementSize;

                        chunkBuffer.position((int) sourcePos);
                        buffer.position((int) destPos);

                        for (int b = 0; b < elementSize; b++) {
                            buffer.put(chunkBuffer.get());
                        }
                    }

                    // Increment localIdx
                    int pos = dimensions - 1;
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

            buffer.flip();
            return buffer;
        } else {
            throw new UnsupportedOperationException("Unsupported DataLayoutStorage type: " + dataLayoutStorage.getClass().getName());
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