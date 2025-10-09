package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.datatype.ReferenceDatatype;
import org.hdf5javalib.hdfjava.HdfDataObject;
import org.hdf5javalib.hdfjava.HdfTree;
import org.hdf5javalib.hdfjava.HdfTreeNode;
import org.hdf5javalib.utils.HdfDataHolder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class HdfDatasetRegionReference implements HdfReferenceInstance {
    private final boolean external;
    private final ReferenceDatatype.ReferenceType referenceType;
    private final HdfDataObject hdfDataObject;
    private final HdfDataspaceSelectionInstance dataspaceSelectionInstance;
    private final HdfDataHolder hdfDataHolder;

    public HdfDatasetRegionReference(byte[] bytes, ReferenceDatatype dt, boolean external) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.external = external;
        this.referenceType = ReferenceDatatype.ReferenceType.DATASET_REGION1;
        FixedPointDatatype offsetSpec = dt.getDataFile().getSuperblock().getFixedPointDatatypeForOffset();
        int offsetSize = offsetSpec.getSize();
        HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 0, offsetSize), offsetSpec);
        ByteBuffer bb = ByteBuffer.wrap(bytes, offsetSize, bytes.length - offsetSize).order(ByteOrder.LITTLE_ENDIAN);
        byte[] dataBytes = dt.getDataFile().getGlobalHeap().getDataBytes(heapOffset, bb.getInt());
        HdfFixedPoint localHdfFixedPoint = new HdfFixedPoint(Arrays.copyOfRange(dataBytes, 0, offsetSize), offsetSpec);


        ByteBuffer remaingData = ByteBuffer.wrap(dataBytes, offsetSize, dataBytes.length - (offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
        dataspaceSelectionInstance = HdfDataspaceSelectionInstance.parseSelectionInfo(remaingData);

        AtomicReference<HdfDataObject> localHdfDataObject = new AtomicReference<>();
        HdfTree bTree = dt.getDataFile().getBTree();
        for (HdfTreeNode node : bTree) {
            HdfFixedPoint objectOffset = node.getDataObject().getObjectHeader().getOffset();
            if (objectOffset.compareTo(localHdfFixedPoint) == 0) {
                localHdfDataObject.set(node.getDataObject());
                break;
            }
        }

        hdfDataObject = localHdfDataObject.get();
        hdfDataHolder = dataspaceSelectionInstance.getData(hdfDataObject, dt.getDataFile());
    }

    @Override
    public String toString() {
        return "HdfDatasetRegionReference [\r\n\texternal=" + external + ", "
                + "\r\n\treferenceType=" + referenceType.getDescription()
                + "\r\n\thdfDataObject=" + (hdfDataObject == null ? "ObjectId not decoded" : "ObjectName: " + hdfDataObject.getObjectName())
                + "\r\n\tdataspaceSelectionInstance=" + (dataspaceSelectionInstance == null ? "no Dataspace Selection" : dataspaceSelectionInstance)
                + "\r\n]";

    }

    @Override
    public HdfDataHolder getData() {
        return hdfDataHolder;
    }
}
