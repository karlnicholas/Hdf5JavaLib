package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.datatype.Datatype;
import org.hdf5javalib.maydo.datatype.ReferenceDatatype;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.AttributeMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DataLayoutMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DataspaceMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DatatypeMessage;

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

    /**
     * Retrieves the dimension sizes of the dataset.
     *
     * @return an array of {@link HdfFixedPoint} representing the dimension sizes
     */
    public Optional<HdfFixedPoint[]> getDimensionSizes() {
        return objectHeader.findMessageByType(DataLayoutMessage.class)
                .flatMap(dataLayoutMessage -> Optional.ofNullable(dataLayoutMessage.getDimensionSizes()));
    }

    /**
     * Retrieves the data address of the dataset.
     *
     * @return the {@link org.hdf5javalib.redo.dataclass.HdfFixedPoint} representing the data address
     */
    public Optional<HdfFixedPoint> getDataAddress() {
        return objectHeader.findMessageByType(DataLayoutMessage.class)
                .flatMap(dataLayoutMessage -> Optional.ofNullable(dataLayoutMessage.getDataAddress()));
    }


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
}