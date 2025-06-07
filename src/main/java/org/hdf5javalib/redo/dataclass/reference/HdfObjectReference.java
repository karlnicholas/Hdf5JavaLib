package org.hdf5javalib.redo.dataclass.reference;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.datatype.FixedPointDatatype;
import org.hdf5javalib.redo.datatype.ReferenceDatatype;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataObject;
import org.hdf5javalib.redo.hdffile.infrastructure.*;
import org.hdf5javalib.redo.hdffile.metadata.HdfSuperblock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class HdfObjectReference implements HdfReferenceInstance {
    private static final Logger log = LoggerFactory.getLogger(HdfObjectReference.class);
    private final boolean external;
    private final ReferenceDatatype.ReferenceType referenceType;
    private final HdfDataObject hdfDataObject;
    private final HdfDataspaceSelectionInstance dataspaceSelectionInstance;

    public HdfObjectReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
        AtomicReference<HdfDataObject> localHdfDataObject = new AtomicReference<>();
        AtomicReference<HdfDataspaceSelectionInstance> dataspaceSelectionReference = new AtomicReference<>();

        HdfFixedPoint localHdfFixedPoint;
        HdfSuperblock superblock = dt.getDataFile().getFileAllocation().getSuperblock();
        referenceType = ReferenceDatatype.getReferenceType(dt.getClassBitField());
        if ( !external) {
            if ( referenceType ==  ReferenceDatatype.ReferenceType.OBJECT1) {
                localHdfFixedPoint = new HdfFixedPoint(bytes, superblock.getFixedPointDatatypeForOffset());
            } else if ( referenceType == ReferenceDatatype.ReferenceType.OBJECT2) {
                if ( bytes[0] == 0x02) {
                    int size = Byte.toUnsignedInt(bytes[2]);
                    localHdfFixedPoint = new HdfFixedPoint(Arrays.copyOfRange(bytes, 3, 3 + size), superblock.getFixedPointDatatypeForOffset());
                } else if ( bytes[0] == 0x03) {
                    FixedPointDatatype offsetSpec = dt.getDataFile().getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset();
                    int offsetSize = offsetSpec.getSize();
                    ByteBuffer bb = ByteBuffer.wrap(bytes, 2+4+offsetSize, bytes.length - (2+4+offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
                    HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 2+4, 2+4+offsetSize) , offsetSpec);
                    int index = bb.getInt();
                    byte[] dataBytes = dt.getDataFile().getGlobalHeap().getDataBytes(heapOffset, index);
                    ByteBuffer heapBytes = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN);
                    int tokenLength = Byte.toUnsignedInt(heapBytes.get());
                    byte[] datasetReferenceBytes = new byte[tokenLength];
                    heapBytes.get(datasetReferenceBytes);
                    HdfFixedPoint datasetReferenced = new HdfFixedPoint(datasetReferenceBytes, offsetSpec);
                    long length = Integer.toUnsignedLong(heapBytes.getInt());
                    int selectionType = heapBytes.getInt();
                    dataspaceSelectionReference.set(HdfDataspaceSelectionInstance.parseSelectionInfo(heapBytes));
                    localHdfFixedPoint = datasetReferenced;
                } else if ( bytes[0] == 0x04) {
                    FixedPointDatatype offsetSpec = dt.getDataFile().getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset();
                    int offsetSize = offsetSpec.getSize();
                    ByteBuffer bb = ByteBuffer.wrap(bytes, 2+4+offsetSize, bytes.length - (2+4+offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
                    HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 2+4, 2+4+offsetSize) , offsetSpec);
                    int index = bb.getInt();
                    byte[] dataBytes = dt.getDataFile().getGlobalHeap().getDataBytes(heapOffset, index);
                    ByteBuffer heapBytes = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN);
                    int tokenLength = Byte.toUnsignedInt(heapBytes.get());
                    byte[] datasetReferenceBytes = new byte[tokenLength];
                    heapBytes.get(datasetReferenceBytes);
                    HdfFixedPoint datasetReferenced = new HdfFixedPoint(datasetReferenceBytes, offsetSpec);
                    long length = Integer.toUnsignedLong(heapBytes.getInt());
                    int nameLength = heapBytes.getInt();
                    byte[] nameBytes = new byte[nameLength];
                    heapBytes.get(nameBytes);
                    String attributeName = new String(nameBytes);
                    HdfSelectionAttribute selectionAttribute = new HdfSelectionAttribute(attributeName);
                    dataspaceSelectionReference.set(selectionAttribute);
                    localHdfFixedPoint = datasetReferenced;
                } else {
                    throw new IllegalArgumentException("Invalid reference type");
                }
            } else {
                throw new IllegalArgumentException("Unsupported reference type: " + dt.getClassBitField());
            }
            if ( localHdfFixedPoint != null ) {
                // TODO: btree search logic
                HdfSymbolTableEntry rootSte = dt.getDataFile().getFileAllocation().getSuperblock().getRootGroupSymbolTableEntry();
                HdfBTreeV1 btree = ((HdfSymbolTableEntryCacheGroupMetadata) rootSte.getCache()).getBtree();
                btree.mapOffsetToSnod().values().forEach(snod->{
                    snod.getSymbolTableEntries().forEach(ste->{
                        HdfFixedPoint objectOffset = ste.getObjectHeader().getOffset();
                        if ( objectOffset.compareTo(localHdfFixedPoint) == 0) {
                            HdfSymbolTableEntryCache cache = ste.getCache();
                            if ( cache.getCacheType() == 0) {
                                localHdfDataObject.set(((HdfSymbolTableEntryCacheNotUsed) cache).getDataSet());
                            } else if ( cache.getCacheType() == 1) {
                                localHdfDataObject.set(((HdfSymbolTableEntryCacheGroupMetadata) cache).getGroup());
                            } else {
                                throw new IllegalStateException("reference type not a good type: " + cache.getCacheType());
                            }
                        }
                    });
                });
            }
        }
        this.dataspaceSelectionInstance = dataspaceSelectionReference.get();
        this.hdfDataObject = localHdfDataObject.get();
    }

    @Override
    public String toString() {
        return "HdfObjectReference [\r\n\texternal=" + external + ", "
                + "\r\n\treferenceType=" + referenceType.getDescription()
                + "\r\n\thdfDataObject=" + (hdfDataObject == null ? "ObjectId not decoded" : "ObjectName: " + hdfDataObject.getObjectName())
                + ( referenceType.getValue() > ReferenceDatatype.ReferenceType.DATASET_REGION1.getValue() ?
                    "\r\n\tdataspaceSelectionInstance=" + (dataspaceSelectionInstance == null ? "no Dataspace Selection" : dataspaceSelectionInstance)
                            : "" )
                + "\r\n]";

    }
}
