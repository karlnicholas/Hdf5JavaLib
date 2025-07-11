package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.datatype.Datatype;
import org.hdf5javalib.maydo.datatype.ReferenceDatatype;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.AttributeMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DataLayoutMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DataspaceMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DatatypeMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Represents a leaf node (HdfDataset) in the B-Tree.
 */
public class HdfDataset extends HdfDataObject implements AutoCloseable {

    public HdfDataset(String objectName, HdfObjectHeaderPrefix objectHeader, HdfBTreeNode parent) {
        super(objectName, objectHeader, parent);
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

    }

    public void createAttribute(String attributeName, String attributeValue, HdfDataFile hdfDataFile) {
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
        return Collections.emptyList();
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
    public int[] extractDimensions() {
        return objectHeader.findMessageByType(DataspaceMessage.class).map(dataspace -> {
            HdfFixedPoint[] dims = dataspace.getDimensions();
            int[] result = new int[dims.length];
            for (int i = 0; i < dims.length; i++) {
                result[i] = dims[i].getInstance(Long.class).intValue();
            }
            return result;
        }).orElse(new int[0]);
    }

    public int getElementSize() {
        return getDatatype().getSize();
    }

    public synchronized ByteBuffer getDatasetData(SeekableByteChannel channel, long offset, long size) throws IOException {
        return objectHeader.findMessageByType(DataLayoutMessage.class).orElseThrow().getData(channel, offset, size);
    }

    public boolean hasData() {
        return hasDataspaceMessage()
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