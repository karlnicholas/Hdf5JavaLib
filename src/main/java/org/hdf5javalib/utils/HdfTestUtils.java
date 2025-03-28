package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.util.BitSet;

public class HdfTestUtils {
    public static void writeVersionAttribute(HdfDataSet dataset) {
        String ATTRIBUTE_NAME = "GIT root revision";
        String ATTRIBUTE_VALUE = "Revision: , URL: ";
        BitSet classBitField = StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII);
        // value
        StringDatatype attributeType = new StringDatatype(StringDatatype.createClassAndVersion(), classBitField, (short)ATTRIBUTE_VALUE.length());
        // data type, String, DATASET_NAME.length
        short dataTypeMessageSize = 8;
        dataTypeMessageSize += attributeType.getSizeMessageData();
        // to 8 byte boundary
        dataTypeMessageSize += ((dataTypeMessageSize + 7) & ~7);
        DatatypeMessage dt = new DatatypeMessage(attributeType, (byte)1, dataTypeMessageSize);
        HdfFixedPoint[] hdfDimensions = {};
        // scalar, 1 string
        short dataSpaceMessageSize = 8;
        DataspaceMessage ds = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false), null, null, false, (byte)0, dataSpaceMessageSize);
        HdfString hdfString = new HdfString(ATTRIBUTE_VALUE.getBytes(), attributeType);
        dataset.createAttribute(ATTRIBUTE_NAME, dt, ds, hdfString);
    }

}
