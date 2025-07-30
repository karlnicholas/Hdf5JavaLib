package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.datatype.ReferenceDatatype;
import org.hdf5javalib.datatype.StringDatatype;
import org.hdf5javalib.hdffile.metadata.HdfSuperblock;
import org.hdf5javalib.hdfjava.HdfBTree;
import org.hdf5javalib.hdfjava.HdfBTreeNode;
import org.hdf5javalib.hdfjava.HdfDataObject;
import org.hdf5javalib.utils.HdfDataHolder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class HdfObjectReference implements HdfReferenceInstance {
    private final boolean external;
    private final ReferenceDatatype.ReferenceType referenceType;
    private final HdfDataObject hdfDataObject;
    private final HdfDataHolder hdfDataHolder;
    private final HdfDataspaceSelectionInstance dataspaceSelectionInstance;

    public HdfObjectReference(byte[] bytes, ReferenceDatatype dt, boolean external) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.external = external;
        AtomicReference<HdfDataObject> localHdfDataObject = new AtomicReference<>();
        AtomicReference<HdfDataspaceSelectionInstance> dataspaceSelectionReference = new AtomicReference<>();

        HdfFixedPoint localHdfFixedPoint;
        HdfSuperblock superblock = dt.getDataFile().getSuperblock();
        referenceType = ReferenceDatatype.getReferenceType(dt.getClassBitField());
        if (!external) {
            if (referenceType == ReferenceDatatype.ReferenceType.OBJECT1) {
                localHdfFixedPoint = new HdfFixedPoint(bytes, superblock.getFixedPointDatatypeForOffset());
            } else if (referenceType == ReferenceDatatype.ReferenceType.OBJECT2) {
                if (bytes[0] == 0x02) {
                    int size = Byte.toUnsignedInt(bytes[2]);
                    localHdfFixedPoint = new HdfFixedPoint(Arrays.copyOfRange(bytes, 3, 3 + size), superblock.getFixedPointDatatypeForOffset());
                } else if (bytes[0] == 0x03) {
                    FixedPointDatatype offsetSpec = dt.getDataFile().getSuperblock().getFixedPointDatatypeForOffset();
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
                    FixedPointDatatype offsetSpec = dt.getDataFile().getSuperblock().getFixedPointDatatypeForOffset();
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
            HdfBTree bTree = dt.getDataFile().getBTree();
            for (HdfBTreeNode node : bTree) {
                HdfFixedPoint objectOffset = node.getDataObject().getObjectHeader().getOffset();
                if (objectOffset.compareTo(localHdfFixedPoint) == 0) {
                    localHdfDataObject.set(node.getDataObject());
                    break;
                }
            }
        } else {
            throw new IllegalArgumentException("Unsupported reference type: " + dt.getClassBitField());
        }
        this.dataspaceSelectionInstance = dataspaceSelectionReference.get();
        this.hdfDataObject = localHdfDataObject.get();
        if ( dataspaceSelectionInstance != null ) {
            this.hdfDataHolder = dataspaceSelectionInstance.getData(hdfDataObject, dt.getDataFile());
        } else {
            List<String> parents = new ArrayList<>();
            HdfDataObject currentNode = hdfDataObject;
            while(currentNode.getParent() != null) {
                parents.add(currentNode.getObjectName());
                currentNode = currentNode.getParent().getDataObject();
            }
            Collections.reverse(parents);
            String objectPathString = '/' + currentNode.getObjectName() + String.join("/", parents);
            this.hdfDataHolder = HdfDataHolder.ofScalar(
                    new HdfString(objectPathString, new StringDatatype(
                    StringDatatype.createClassAndVersion(),
                    StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII),
                            objectPathString.length(),
                    dt.getDataFile())
            ));
        }
    }

    @Override
    public String toString() {
        return "HdfObjectReference [\r\n\texternal=" + external + ", "
                + "\r\n\treferenceType=" + referenceType.getDescription()
                + "\r\n\thdfDataObject=" + (hdfDataObject == null ? "ObjectId not decoded" : "ObjectName: " + hdfDataObject.getObjectName())
                + (referenceType.getValue() > ReferenceDatatype.ReferenceType.DATASET_REGION1.getValue() ?
                "\r\n\tdataspaceSelectionInstance=" + (Objects.requireNonNullElse(dataspaceSelectionInstance, "no Dataspace Selection"))
                : "")
                + "\r\n]";

    }

    @Override
    public HdfDataHolder getData() {
        return hdfDataHolder;
    }
}
