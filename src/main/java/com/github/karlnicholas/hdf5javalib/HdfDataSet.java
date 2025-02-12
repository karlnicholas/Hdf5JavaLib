package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.message.AttributeMessage;
import com.github.karlnicholas.hdf5javalib.message.DataSpaceMessage;
import com.github.karlnicholas.hdf5javalib.message.DataTypeMessage;
import com.github.karlnicholas.hdf5javalib.utils.HdfDataSource;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

public class HdfDataSet {
    private final H5File hdfFile;

    public HdfDataSet(H5File hdfFile) {
        this.hdfFile = hdfFile;
    }

    public void write(List<VolumeData> shipments, CompoundDataType compoundType, Class clazz) {
        HdfDataSource hdfDataSource = new HdfDataSource(compoundType, clazz);
        ByteBuffer buffer = ByteBuffer.allocate(compoundType.getSize());
        for (VolumeData shipment : shipments) {
            hdfDataSource.writeToBuffer(shipment, buffer);
            buffer.flip();
            hdfFile.write(buffer);
            buffer.rewind();
        }
    }

    public void close() {
    }

    public AttributeMessage createAttribute(String attributeName, HdfFixedPoint attrType, HdfFixedPoint[] attrSpace) {
        DataTypeMessage dt = new DataTypeMessage(1, 3, BitSet.valueOf(new byte[0]), attrType, new HdfString(attributeName, false));
        DataSpaceMessage ds = new DataSpaceMessage(1, 1, 1, attrSpace, null, false);
        return new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), new HdfString(attributeName, false));
    }
}
