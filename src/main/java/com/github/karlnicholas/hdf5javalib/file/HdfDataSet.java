package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.message.AttributeMessage;
import com.github.karlnicholas.hdf5javalib.message.DataSpaceMessage;
import com.github.karlnicholas.hdf5javalib.message.DataTypeMessage;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

@Getter
public class HdfDataSet<T> {
    private final HdfFile hdfFile;
    private String datasetName;
    private CompoundDataType compoundDataType;
    private HdfFixedPoint[] hdfDimensions;
    private List<AttributeMessage> attributes;

    public HdfDataSet(HdfFile hdfFile, String datasetName, CompoundDataType compoundType, HdfFixedPoint[] hdfDimensions) {
        this.hdfFile = hdfFile;
        this.datasetName = datasetName;
        this.compoundDataType = compoundType;
        this.hdfDimensions = hdfDimensions;
        this.attributes = new ArrayList<>();
    }

    public void write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        hdfFile.write(bufferSupplier, this);
    }

    public void createAttribute(String attributeName, HdfFixedPoint attrType, HdfFixedPoint[] attrSpace) {
        DataTypeMessage dt = new DataTypeMessage(1, 3, BitSet.valueOf(new byte[0]), attrType, new HdfString(attributeName, false));
        DataSpaceMessage ds = new DataSpaceMessage(1, 1, 1, attrSpace, null, false);
        attributes.add(new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), new HdfString(attributeName, false)));
    }

    public List<AttributeMessage> getAttributes() {
        return attributes;
    }
}
