package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.dataclass.HdfString;
import org.hdf5javalib.maydo.datatype.FixedPointDatatype;
import org.hdf5javalib.maydo.datatype.ReferenceDatatype;
import org.hdf5javalib.maydo.datatype.StringDatatype;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.hdfjava.HdfDataObject;
import org.hdf5javalib.maydo.hdffile.infrastructure.*;
import org.hdf5javalib.maydo.hdffile.metadata.HdfSuperblock;
import org.hdf5javalib.maydo.hdfjava.HdfGroup;
import org.hdf5javalib.maydo.utils.HdfDataHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HdfObjectReference implements HdfReferenceInstance {
    private static final Logger log = LoggerFactory.getLogger(HdfObjectReference.class);
    private final boolean external;
    private final ReferenceDatatype.ReferenceType referenceType;
    private final HdfDataObject hdfDataObject;
    private final HdfDataHolder hdfDataHolder;
    private final HdfDataspaceSelectionInstance dataspaceSelectionInstance;

    public HdfObjectReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
        AtomicReference<HdfDataObject> localHdfDataObject = new AtomicReference<>();
        AtomicReference<HdfDataspaceSelectionInstance> dataspaceSelectionReference = new AtomicReference<>();

        HdfFixedPoint localHdfFixedPoint;
        HdfSuperblock superblock = dt.getDataFile().getFileAllocation().getSuperblock();
        referenceType = ReferenceDatatype.getReferenceType(dt.getClassBitField());
        if (!external) {
            if (referenceType == ReferenceDatatype.ReferenceType.OBJECT1) {
                localHdfFixedPoint = new HdfFixedPoint(bytes, superblock.getFixedPointDatatypeForOffset());
            } else if (referenceType == ReferenceDatatype.ReferenceType.OBJECT2) {
                if (bytes[0] == 0x02) {
                    int size = Byte.toUnsignedInt(bytes[2]);
                    localHdfFixedPoint = new HdfFixedPoint(Arrays.copyOfRange(bytes, 3, 3 + size), superblock.getFixedPointDatatypeForOffset());
                } else if (bytes[0] == 0x03) {
                    FixedPointDatatype offsetSpec = dt.getDataFile().getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset();
                    int offsetSize = offsetSpec.getSize();
                    ByteBuffer bb = ByteBuffer.wrap(bytes, 2 + 4 + offsetSize, bytes.length - (2 + 4 + offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
                    HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 2 + 4, 2 + 4 + offsetSize), offsetSpec);
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
                } else if (bytes[0] == 0x04) {
                    FixedPointDatatype offsetSpec = dt.getDataFile().getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset();
                    int offsetSize = offsetSpec.getSize();
                    ByteBuffer bb = ByteBuffer.wrap(bytes, 2 + 4 + offsetSize, bytes.length - (2 + 4 + offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
                    HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 2 + 4, 2 + 4 + offsetSize), offsetSpec);
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
            if (localHdfFixedPoint != null) {
                // TODO: btree search logic
                HdfSymbolTableEntry rootSte = dt.getDataFile().getFileAllocation().getSuperblock().getRootGroupSymbolTableEntry();
                HdfBTreeV1 btree = ((HdfSymbolTableEntryCacheWithScratch) rootSte.getCache()).getBtree();
                btree.mapOffsetToSnod().values().forEach(snod -> {
                    snod.getSymbolTableEntries().forEach(ste -> {
                        HdfFixedPoint objectOffset = ste.getObjectHeader().getDataObjectAllocationRecord().getOffset();
                        if (objectOffset.compareTo(localHdfFixedPoint) == 0) {
                            HdfSymbolTableEntryCache cache = ste.getCache();
                            if (cache.getCacheType() == 0) {
                                localHdfDataObject.set(((HdfSymbolTableEntryCacheNotUsed) cache).getDataSet());
                            } else if (cache.getCacheType() == 1) {
                                localHdfDataObject.set(((HdfSymbolTableEntryCacheWithScratch) cache).getGroup());
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
        if ( dataspaceSelectionInstance != null ) {
            this.hdfDataHolder = dataspaceSelectionInstance.getData(hdfDataObject, dt.getDataFile());
        } else {
            HdfSymbolTableEntry rootSte = dt.getDataFile().getFileAllocation().getSuperblock().getRootGroupSymbolTableEntry();
            HdfBTreeV1 btree = ((HdfSymbolTableEntryCacheWithScratch) rootSte.getCache()).getBtree();
            HdfGroup rootGroup = ((HdfSymbolTableEntryCacheWithScratch) rootSte.getCache()).getGroup();
            Optional<Deque<HdfDataObject>> objectPath = btree.findObjectPathByName(hdfDataObject.getObjectName(), rootGroup);
            String objectPathString;
            if (objectPath.isPresent()) {
                objectPathString = convertObjectPathToString(objectPath);
            } else {
                objectPathString = hdfDataObject.getObjectName();
            }
            this.hdfDataHolder = HdfDataHolder.ofScalar(
                    new HdfString(objectPathString, new StringDatatype(
                    StringDatatype.createClassAndVersion(),
                    StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII),
                    hdfDataObject.getObjectName().length(),
                    dt.getDataFile())
            ));
        }
    }

    /**
     * Converts a Deque of HdfDataObjects into a string path with '/' as separator,
     * using getObjectName() for each object, always starting with '/'.
     *
     * @param objectPath an Optional containing a Deque of HdfDataObjects
     * @return a string starting with '/', with object names joined by '/', or "/" if not present or empty
     * @throws NullPointerException if any object's getObjectName() returns null
     */
    public String convertObjectPathToString(Optional<Deque<HdfDataObject>> objectPath) {
        if (objectPath.isEmpty() || objectPath.get().isEmpty()) {
            return "/";
        }
        String path = objectPath.get().stream()
                .map(obj -> {
                    String name = obj.getObjectName();
                    if (name == null) {
                        throw new NullPointerException("Object name cannot be null in path");
                    }
                    return name;
                })
                .collect(Collectors.joining("/"));
        return "/" + path;
    }

    @Override
    public String toString() {
        return "HdfObjectReference [\r\n\texternal=" + external + ", "
                + "\r\n\treferenceType=" + referenceType.getDescription()
                + "\r\n\thdfDataObject=" + (hdfDataObject == null ? "ObjectId not decoded" : "ObjectName: " + hdfDataObject.getObjectName())
                + (referenceType.getValue() > ReferenceDatatype.ReferenceType.DATASET_REGION1.getValue() ?
                "\r\n\tdataspaceSelectionInstance=" + (dataspaceSelectionInstance == null ? "no Dataspace Selection" : dataspaceSelectionInstance)
                : "")
                + "\r\n]";

    }

    @Override
    public HdfDataHolder getData(HdfDataFile dataFile) {
        return hdfDataHolder;
    }
}
