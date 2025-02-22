package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfDatatype;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.message.*;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;

@Getter
public class HdfDataSet {
    private final HdfGroup hdfGroup;
    private final String datasetName;
    private final HdfDatatype hdfDatatype;
    private final List<AttributeMessage> attributes;
    private final DataspaceMessage dataSpaceMessage;
    private long dataAddress;
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
        this.dataSpaceMessage = dataSpaceMessage;
    }

    public void write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        dataAddress = hdfGroup.write(bufferSupplier, this);
    }

//    public AttributeMessage createAttribute(String attributeName, HdfFixedPoint attrType, HdfFixedPoint[] attrSpace) {
//        DatatypeMessage dt = new DatatypeMessage(1, 3, BitSet.valueOf(new byte[0]), attrType, new HdfString(attributeName, false));
//        DataspaceMessage ds = new DataspaceMessage(1, 1, 1, attrSpace, null, false);
//        AttributeMessage attributeMessage = new AttributeMessage(1, attributeName.length(), 8, 8, dt, ds, new HdfString(attributeName, false), null);
//        attributes.add(attributeMessage);
//        return attributeMessage;
//    }

    public void close() {
        // Initialize the localHeapContents heapData array
        // System.arraycopy(hdfDataSet.getDatasetName().getBytes(StandardCharsets.US_ASCII), 0, hdfFile.getLocalHeapContents().getHeapData(), 8, hdfDataSet.getDatasetName().length());
//        long recordCount = dataSpaceMessage.
        int currentObjectHeaderSize = hdfGroup.getHdfFile().getDataGroupStorageSize();
        List<HdfMessage> headerMessages = new ArrayList<>();
        headerMessages.add(dataSpaceMessage);
//        headerMessages.add(new ObjectHeaderContinuationMessage(HdfFixedPoint.of(100208), HdfFixedPoint.of(112)));
//        headerMessages.add(new NilMessage());

        DatatypeMessage dataTypeMessage = new DatatypeMessage((byte) 1, (byte) hdfDatatype.getDatatypeClass().getValue(),
                BitSet.valueOf(new byte[]{0b10001}),
                hdfDatatype.getSize(),
                hdfDatatype);
//        dataTypeMessage.setDataType(compoundType);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        headerMessages.add(new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]));

        // Add DataLayoutMessage (Storage format)
        HdfFixedPoint[] dimensions = dataSpaceMessage.getDimensions();
        long recordCount = dimensions[0].getBigIntegerValue().longValue();
        HdfFixedPoint[] hdfDimensionSizes = { HdfFixedPoint.of(recordCount * hdfDatatype.getSize())};
        DataLayoutMessage dataLayoutMessage = new DataLayoutMessage(3, 1,
                HdfFixedPoint.of(hdfGroup.getHdfFile().getDataAddress()),
                hdfDimensionSizes,
                0, null, HdfFixedPoint.undefined((short)8));
        headerMessages.add(dataLayoutMessage);

        // add ObjectModification Time message
        headerMessages.add(new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond()));

//        // Add DataspaceMessage (Handles dataset dimensionality)
//        HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(recordCount)};

//        DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
//        headerMessages.add(dataSpaceMessage);

        headerMessages.addAll(attributes);

        // new long[]{1750}, new long[]{98000}
        int objectReferenceCount = 1;
        int objectHeaderSize = 0;
        // 8, 1, 1064
        for( HdfMessage headerMessage: headerMessages ) {
            objectHeaderSize += headerMessage.getSizeMessageData();
        }
        if ( objectHeaderSize > currentObjectHeaderSize) {
            List<HdfMessage> newMessages = new ArrayList<>();
            ObjectHeaderContinuationMessage objectHeaderContinuationMessage = new ObjectHeaderContinuationMessage(HdfFixedPoint.of(0), HdfFixedPoint.of(0));
            newMessages.add(objectHeaderContinuationMessage);
//            newMessages.add(new NilMessage());
//            newMessages.addAll(headerMessages);
            headerMessages = newMessages;
            int breakPostion = 0;
            int breakSize = 0;
            while (breakPostion < headerMessages.size()) {
                breakSize += headerMessages.get(breakPostion).getSizeMessageData();
                breakPostion++;
                if ( breakSize > currentObjectHeaderSize ) {
                    break;
                }
            }
            int continueSize = 0;
            while (breakPostion < headerMessages.size()) {
                continueSize += headerMessages.get(breakPostion).getSizeMessageData();
                breakPostion++;
            }
//            long endOfData = (hdfFile.getDatasetRecordCount().get() * compoundDataType.getSize()) + hdfFile.getDataAddress();
//            objectHeaderContinuationMessage.setContinuationOffset(HdfFixedPoint.of(endOfData));
            objectHeaderContinuationMessage.setContinuationOffset(HdfFixedPoint.undefined((short)8));
            objectHeaderContinuationMessage.setContinuationSize(HdfFixedPoint.of(continueSize));
        }
        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, headerMessages.size(), objectReferenceCount, Math.max(objectHeaderSize, currentObjectHeaderSize), headerMessages);
        System.out.println("HDF5 dataset message" + dataObjectHeaderPrefix);

    }
//    public long updateForRecordCount(long l) {
//
////        dataObjectHeaderPrefix.findHdfSymbolTableMessage(DataLayoutMessage.class)
////                .orElseThrow()
////                .getDimensionSizes()
//        return hdfFile.getDatasetRecordCount().get() + l;
//    }
}
