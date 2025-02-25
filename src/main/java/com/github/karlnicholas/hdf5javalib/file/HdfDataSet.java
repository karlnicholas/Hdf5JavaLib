package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.data.HdfData;
import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.data.HdfString;
import com.github.karlnicholas.hdf5javalib.datatype.HdfDatatype;
import com.github.karlnicholas.hdf5javalib.datatype.StringDatatype;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.message.*;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

    public AttributeMessage createAttribute(String name, DatatypeMessage dt, DataspaceMessage ds, HdfData value) {
        byte[] nameBytes = new byte[name.length()+1];
        System.arraycopy(name.getBytes(StandardCharsets.UTF_8), 0, nameBytes, 0, name.length());
        AttributeMessage attributeMessage = new AttributeMessage(1,
                new HdfString(nameBytes, StringDatatype.getStringTypeBitSet(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII)),
                dt, ds, value);
        attributes.add(attributeMessage);
        return attributeMessage;
    }

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
                hdfDatatype.getClassBitBytes(),
                hdfDatatype.getSize(),
                hdfDatatype);
//        dataTypeMessage.setDataType(compoundType);
        headerMessages.add(dataTypeMessage);

        // Add FillValue message
        FillValueMessage fillValueMessage = new FillValueMessage(2, 2, 2, 1, HdfFixedPoint.of(0), new byte[0]);
        headerMessages.add(fillValueMessage);

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
        ObjectModificationTimeMessage objectModificationTimeMessage = new ObjectModificationTimeMessage(1, Instant.now().getEpochSecond());
        headerMessages.add(objectModificationTimeMessage);

        // attribute messages at the end
        headerMessages.addAll(attributes);
//        // Add DataspaceMessage (Handles dataset dimensionality)
//        HdfFixedPoint[] hdfDimensions = {HdfFixedPoint.of(recordCount)};

//        DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, 1, hdfDimensions, hdfDimensions, true);
//        headerMessages.add(dataSpaceMessage);

        // new long[]{1750}, new long[]{98000}
        int objectReferenceCount = 1;
        int objectHeaderSize = 0;
        // 8, 1, 1064
        for( HdfMessage headerMessage: headerMessages ) {
            objectHeaderSize += headerMessage.getSizeMessageData() + 8;
        }
        // Test whether there is space enough for a NilMessage of 0 length
        if ( objectHeaderSize + 8 > currentObjectHeaderSize) {
            // how often does this happen?
//            currentObjectHeaderSize = 1024;
//            hdfGroup.getHdfFile().setDataGroupStorageSize(currentObjectHeaderSize);

            // restructure the messages
            headerMessages.clear();
            ObjectHeaderContinuationMessage objectHeaderContinuationMessage = new ObjectHeaderContinuationMessage(HdfFixedPoint.of(0), HdfFixedPoint.of(0));
            headerMessages.add(objectHeaderContinuationMessage);
            // NiLMessage is now 0 size because there is no extra space
            headerMessages.add(new NilMessage(0));
            headerMessages.add(dataTypeMessage);
            headerMessages.add(fillValueMessage);
            headerMessages.add(dataLayoutMessage);
            headerMessages.add(objectModificationTimeMessage);
            headerMessages.add(dataSpaceMessage);
            headerMessages.addAll(attributes);

            int breakPostion = headerMessages.size() - 2;
//            int breakSize = 0;
//            while (breakPostion < headerMessages.size()) {
//                breakSize += headerMessages.get(breakPostion).getSizeMessageData() + 8;
//                breakPostion++;
//                if ( breakSize > 1024 ) {
//                    break;
//                }
//            }
            int continueSize = 0;
            while (breakPostion < headerMessages.size()) {
                continueSize += headerMessages.get(breakPostion).getSizeMessageData() + 8;
                breakPostion++;
            }
//            long endOfData = (hdfFile.getDatasetRecordCount().get() * compoundDataType.getSize()) + hdfFile.getDataAddress();
//            objectHeaderContinuationMessage.setContinuationOffset(HdfFixedPoint.of(endOfData));
            objectHeaderContinuationMessage.setContinuationOffset(HdfFixedPoint.undefined((short)8));
            objectHeaderContinuationMessage.setContinuationSize(HdfFixedPoint.of(continueSize));
        } else {
            // add remaining space
            headerMessages.add(new NilMessage(currentObjectHeaderSize - 8 - objectHeaderSize));

        }
        this.dataObjectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, headerMessages.size(), objectReferenceCount, Math.max(objectHeaderSize, currentObjectHeaderSize), headerMessages);
        System.out.println(datasetName + "@" + hdfGroup.getHdfFile().getDataGroupAddress() + " = " + dataObjectHeaderPrefix);

    }

    public void writeToBuffer(ByteBuffer buffer) {
        dataObjectHeaderPrefix.writeToByteBuffer(buffer);
    }
//    public long updateForRecordCount(long l) {
//
////        dataObjectHeaderPrefix.findHdfSymbolTableMessage(DataLayoutMessage.class)
////                .orElseThrow()
////                .getDimensionSizes()
//        return hdfFile.getDatasetRecordCount().get() + l;
//    }
}
