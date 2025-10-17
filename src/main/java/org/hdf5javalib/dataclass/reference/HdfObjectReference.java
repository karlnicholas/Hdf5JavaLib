package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.datatype.ReferenceDatatype;
import org.hdf5javalib.datatype.StringDatatype;
import org.hdf5javalib.hdffile.metadata.HdfSuperblock;
import org.hdf5javalib.hdfjava.HdfDataObject;
import org.hdf5javalib.hdfjava.HdfTree;
import org.hdf5javalib.hdfjava.HdfTreeNode;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;
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

        if (external) {
            throw new IllegalArgumentException("Unsupported reference type: " + dt.getClassBitField());
        }

        // Logic for !external (Internal References)
        if (referenceType == ReferenceDatatype.ReferenceType.OBJECT1) {
            localHdfFixedPoint = new HdfFixedPoint(bytes, superblock.getFixedPointDatatypeForOffset());
        } else if (referenceType == ReferenceDatatype.ReferenceType.OBJECT2) {
            // Delegate complex OBJECT2 parsing to a helper method
            ObjectReferenceData data = parseObject2Reference(bytes, dt);
            localHdfFixedPoint = data.fixedPoint;
            dataspaceSelectionReference.set(data.selectionInstance);
        } else {
            throw new IllegalArgumentException("Unsupported reference type: " + dt.getClassBitField());
        }

        // Object lookup (Shared logic)
        HdfTree bTree = dt.getDataFile().getBTree();
        for (HdfTreeNode node : bTree) {
            HdfFixedPoint objectOffset = node.getDataObject().getObjectHeader().getOffset();
            if (objectOffset.compareTo(localHdfFixedPoint) == 0) {
                localHdfDataObject.set(node.getDataObject());
                break;
            }
        }

        // Final assignments and DataHolder creation (Shared logic)
        this.dataspaceSelectionInstance = dataspaceSelectionReference.get();
        this.hdfDataObject = localHdfDataObject.get();
        this.hdfDataHolder = createDataHolder(this.hdfDataObject, this.dataspaceSelectionInstance, dt, localHdfFixedPoint);
    }

    //
    // Helper Classes and Methods for Complexity Reduction
    //

    private record ObjectReferenceData(HdfFixedPoint fixedPoint, HdfDataspaceSelectionInstance selectionInstance) { }

    /**
     * Extracts the core logic for parsing HDF5 'OBJECT2' references (bytes[0] == 0x02, 0x03, 0x04).
     * This extraction drastically reduces the constructor's cognitive complexity.
     */
    private ObjectReferenceData parseObject2Reference(byte[] bytes, ReferenceDatatype dt) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        return switch (bytes[0]) {
            case 0x02 -> parseObject2Type2(bytes, dt);
            case 0x03 -> parseObject2Type3(bytes, dt);
            case 0x04 -> parseObject2Type4(bytes, dt);
            default -> throw new IllegalArgumentException("Invalid reference type: " + bytes[0]);
        };
    }

    private ObjectReferenceData parseObject2Type2(byte[] bytes, ReferenceDatatype dt) {
        HdfSuperblock superblock = dt.getDataFile().getSuperblock();
        int size = Byte.toUnsignedInt(bytes[2]);
        HdfFixedPoint fixedPoint = new HdfFixedPoint(Arrays.copyOfRange(bytes, 3, 3 + size), superblock.getFixedPointDatatypeForOffset());
        return new ObjectReferenceData(fixedPoint, null);
    }

    private ObjectReferenceData parseObject2Type3(byte[] bytes, ReferenceDatatype dt) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        FixedPointDatatype offsetSpec = dt.getDataFile().getSuperblock().getFixedPointDatatypeForOffset();
        int offsetSize = offsetSpec.getSize();

        // Parse object reference and global heap index
        ByteBuffer bb = ByteBuffer.wrap(bytes, 2 + 4 + offsetSize, bytes.length - (2 + 4 + offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
        HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 2 + 4, 2 + 4 + offsetSize), offsetSpec);
        int index = bb.getInt();

        // Get data from global heap
        byte[] dataBytes = dt.getDataFile().getGlobalHeap().getDataBytes(heapOffset, index);
        ByteBuffer heapBytes = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN);

        // Parse referenced dataset offset
        int tokenLength = Byte.toUnsignedInt(heapBytes.get());
        byte[] datasetReferenceBytes = new byte[tokenLength];
        heapBytes.get(datasetReferenceBytes);
        HdfFixedPoint datasetReferenced = new HdfFixedPoint(datasetReferenceBytes, offsetSpec);

        /* long length = */ heapBytes.getInt(); // Length is read but not used
        /* int selectionType = */ heapBytes.getInt(); // selectionType is read but not used

        HdfDataspaceSelectionInstance selectionInstance = HdfDataspaceSelectionInstance.parseSelectionInfo(heapBytes);

        return new ObjectReferenceData(datasetReferenced, selectionInstance);
    }

    private ObjectReferenceData parseObject2Type4(byte[] bytes, ReferenceDatatype dt) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        FixedPointDatatype offsetSpec = dt.getDataFile().getSuperblock().getFixedPointDatatypeForOffset();
        int offsetSize = offsetSpec.getSize();

        // Parse object reference and global heap index
        ByteBuffer bb = ByteBuffer.wrap(bytes, 2 + 4 + offsetSize, bytes.length - (2 + 4 + offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
        HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 2 + 4, 2 + 4 + offsetSize), offsetSpec);
        int index = bb.getInt();

        // Get data from global heap
        byte[] dataBytes = dt.getDataFile().getGlobalHeap().getDataBytes(heapOffset, index);
        ByteBuffer heapBytes = ByteBuffer.wrap(dataBytes).order(ByteOrder.LITTLE_ENDIAN);

        // Parse referenced dataset offset
        int tokenLength = Byte.toUnsignedInt(heapBytes.get());
        byte[] datasetReferenceBytes = new byte[tokenLength];
        heapBytes.get(datasetReferenceBytes);
        HdfFixedPoint datasetReferenced = new HdfFixedPoint(datasetReferenceBytes, offsetSpec);

        /* long length = */ heapBytes.getInt(); // Length is read but not used
        int nameLength = heapBytes.getInt();

        // Parse attribute name
        byte[] nameBytes = new byte[nameLength];
        heapBytes.get(nameBytes);
        String attributeName = new String(nameBytes);
        HdfSelectionAttribute selectionAttribute = new HdfSelectionAttribute(attributeName);

        return new ObjectReferenceData(datasetReferenced, selectionAttribute);
    }

    /**
     * Centralized logic for creating the final HdfDataHolder.
     */
    private HdfDataHolder createDataHolder(HdfDataObject hdfDataObject, HdfDataspaceSelectionInstance selection, ReferenceDatatype dt, HdfFixedPoint localHdfFixedPoint)
            throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (selection != null) {
            return selection.getData(hdfDataObject, dt.getDataFile());
        }

        if (hdfDataObject == null) {
            throw new IllegalStateException("DataObject for reference not found for: " + localHdfFixedPoint);
        }

        // Construct DataHolder representing the object path
        String objectPathString = hdfDataObject.getObjectPath();
        StringDatatype stringDt = new StringDatatype(
                StringDatatype.createClassAndVersion(),
                StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII),
                objectPathString.length(),
                dt.getDataFile());
        return HdfDataHolder.ofScalar(new HdfString(objectPathString, stringDt));
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