package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.message.*;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

@Getter
public class HdfDataSet {
    private final HdfFile hdfFile;
    private final String datasetName;
    private final CompoundDataType compoundDataType;
    private final List<AttributeMessage> attributes;
    private final HdfFixedPoint datasetAddress;
    private HdfObjectHeaderPrefixV1 dataObjectHeaderPrefix;

    /*
     * So, this is a group ?
     * it has a name, "Demand" off the root group of "/"
     * it has a datatype, "Compound" , with dimensions, attributes, and an address in the HdfFile
     * It should have a localHeap and LocalHeap contents, perhaps.
     */

    public HdfDataSet(HdfFile hdfFile, String datasetName, CompoundDataType compoundType, HdfFixedPoint datasetAddress) {
        this.hdfFile = hdfFile;
        this.datasetName = datasetName;
        this.compoundDataType = compoundType;
        this.attributes = new ArrayList<>();
        this.datasetAddress = datasetAddress;
    }

    public void write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        hdfFile.write(bufferSupplier, this);
    }

    public AttributeMessage createAttribute(String attributeName, HdfFixedPoint attrType, HdfFixedPoint[] attrSpace) {
        DatatypeMessage dt = new DatatypeMessage(1, 3, BitSet.valueOf(new byte[0]), attrType, new HdfString(attributeName, false));
        DataspaceMessage ds = new DataspaceMessage(1, 1, 1, attrSpace, null, false);
        AttributeMessage attributeMessage = new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), null);
        attributes.add(attributeMessage);
        return attributeMessage;
    }

    public void close() {
        // Initialize the localHeapContents heapData array
//        System.arraycopy(hdfDataSet.getDatasetName().getBytes(StandardCharsets.US_ASCII), 0, hdfFile.getLocalHeapContents().getHeapData(), 8, hdfDataSet.getDatasetName().length());

        List<HdfMessage> headerMessages = new ArrayList<>();
//        headerMessages.add(new ObjectHeaderContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
//        headerMessages.add(new NilMessage());

        int cdtSize = compoundDataType.getSize();
        DatatypeMessage dataTypeMessage = new DatatypeMessage(1, 6, BitSet.valueOf(new byte[]{0b10001}), new HdfFixedPoint(false, new byte[]{(byte)cdtSize}, (short)4), compoundDataType);
//        dataTypeMessage.setDataType(compoundType);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        headerMessages.add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] hdfDimensionSizes = { HdfFixedPoint.of(hdfFile.getDatasetRecordCount().get()), HdfFixedPoint.of(hdfFile.getDatasetRecordCount().get()) };
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1, HdfFixedPoint.of(hdfFile.getDataAddress()), hdfDimensionSizes, 0, null, HdfFixedPoint.undefined((short)8));
        headerMessages.add(dataLayoutMessage);

        // add ObjectModification Time message
        headerMessages.add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

        // Add DataspaceMessage (Handles dataset dimensionality)
        HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.undefined((short)8)};

        DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
        headerMessages.add(dataSpaceMessage);

//        headerMessages.addAll(hdfDataSet.getAttributes());

        // new long[]{1750}, new long[]{98000}
        int objectReferenceCount = 1;
        int objectHeaderSize = 0;
        // 8, 1, 1064
        for( HdfMessage headerMessage: headerMessages ) {
            objectHeaderSize += headerMessage.getSizeMessageData();
        }
        if ( objectHeaderSize > 1024) {
            List<HdfMessage> newMessages = new ArrayList<>();
            ObjectHeaderContinuationMessage objectHeaderContinuationMessage = new ObjectHeaderContinuationMessage(HdfFixedPoint.of(0), HdfFixedPoint.of(0));
            newMessages.add(objectHeaderContinuationMessage);
            newMessages.add(new NilMessage());
            newMessages.addAll(headerMessages);
            headerMessages = newMessages;
            int breakPostion = 0;
            int breakSize = 0;
            while (breakPostion < headerMessages.size()) {
                if ( breakSize + headerMessages.get(breakPostion).getSizeMessageData() > 1024 ) {
                    break;
                }
                breakSize += headerMessages.get(breakPostion).getSizeMessageData();
                breakPostion++;
            }
            int continueSize = 0;
            while (breakPostion < headerMessages.size()) {
                continueSize += headerMessages.get(breakPostion).getSizeMessageData();
                breakPostion++;
            }
            long endOfData = (hdfFile.getDatasetRecordCount().get() * compoundDataType.getSize()) + hdfFile.getDataAddress();
            objectHeaderContinuationMessage.setContinuationOffset(HdfFixedPoint.of(endOfData));
            objectHeaderContinuationMessage.setContinuationSize(HdfFixedPoint.of(continueSize));
        }
        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, headerMessages.size(), objectReferenceCount, objectHeaderSize, headerMessages);

    }

//    public long updateForRecordCount(long l) {
//
////        dataObjectHeaderPrefix.findHdfSymbolTableMessage(DataLayoutMessage.class)
////                .orElseThrow()
////                .getDimensionSizes()
//        return hdfFile.getDatasetRecordCount().get() + l;
//    }
}
