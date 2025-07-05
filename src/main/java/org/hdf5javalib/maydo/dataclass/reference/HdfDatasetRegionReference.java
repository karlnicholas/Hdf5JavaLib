package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.datatype.ReferenceDatatype;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.hdfjava.HdfDataObject;
import org.hdf5javalib.maydo.utils.HdfDataHolder;

public class HdfDatasetRegionReference implements HdfReferenceInstance {
    private final boolean external;
    private final ReferenceDatatype.ReferenceType referenceType;
    private final HdfDataObject hdfDataObject;
    private final HdfDataspaceSelectionInstance dataspaceSelectionInstance;
    private final HdfDataHolder hdfDataHolder;
    public HdfDatasetRegionReference() {
        this.external = false;
        this.referenceType = null;
        this.hdfDataObject = null;
        this.dataspaceSelectionInstance = null;
        this.hdfDataHolder = null;
    }

    public HdfDatasetRegionReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = false;
        this.referenceType = null;
        this.hdfDataObject = null;
        this.dataspaceSelectionInstance = null;
        this.hdfDataHolder = null;
//        this.external = external;
//        this.referenceType = ReferenceDatatype.ReferenceType.DATASET_REGION1;
//        FixedPointDatatype offsetSpec = dt.getDataFile().getSuperblock().getFixedPointDatatypeForOffset();
//        int offsetSize = offsetSpec.getSize();
//        HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 0, offsetSize), offsetSpec);
//        ByteBuffer bb = ByteBuffer.wrap(bytes, offsetSize, bytes.length - offsetSize).order(ByteOrder.LITTLE_ENDIAN);
//        byte[] dataBytes = dt.getDataFile().getGlobalHeap().getDataBytes(heapOffset, bb.getInt());
////        HdfFixedPoint datasetReferenced = new HdfFixedPoint(Arrays.copyOfRange(dataBytes, 1, offsetSize+1) , offsetSpec);
//        HdfFixedPoint localHdfFixedPoint = new HdfFixedPoint(Arrays.copyOfRange(dataBytes, 0, offsetSize), offsetSpec);
//
//
//        ByteBuffer remaingData = ByteBuffer.wrap(dataBytes, offsetSize, dataBytes.length - (offsetSize)).order(ByteOrder.LITTLE_ENDIAN);
//        dataspaceSelectionInstance = HdfDataspaceSelectionInstance.parseSelectionInfo(remaingData);
//
//        AtomicReference<HdfDataObject> localHdfDataObject = new AtomicReference<>();
//        if (localHdfFixedPoint != null) {
//            // TODO: btree search logic
//            HdfSymbolTableEntry rootSte = dt.getDataFile().getSuperblock().getRootGroupSymbolTableEntry();
//            HdfBTreeV1 btree = ((HdfSymbolTableEntryCacheWithScratch) rootSte.getCache()).getBtree();
//            btree.mapOffsetToSnod().values().forEach(snod -> {
//                snod.getSymbolTableEntries().forEach(ste -> {
////                    HdfFixedPoint objectOffset = ste.getObjectHeader().getDataObjectAllocationRecord().getOffset();
//                    HdfFixedPoint objectOffset = null;
//                    if (objectOffset.compareTo(localHdfFixedPoint) == 0) {
//                        HdfSymbolTableEntryCache cache = ste.getCache();
//                        if (cache.getCacheType() == 0) {
//                            localHdfDataObject.set(((HdfSymbolTableEntryCacheNoScratch) cache).getDataSet());
//                        } else if (cache.getCacheType() == 1) {
//                            localHdfDataObject.set(((HdfSymbolTableEntryCacheWithScratch) cache).getGroup());
//                        } else {
//                            throw new IllegalStateException("reference type not a good type: " + cache.getCacheType());
//                        }
//                    }
//                });
//            });
//
//        }
//        hdfDataObject = localHdfDataObject.get();
//        hdfDataHolder = dataspaceSelectionInstance.getData(hdfDataObject, dt.getDataFile());

//        hdfDataHolder = HdfDataHolder.ofScalar(
//                new HdfString(hdfDataObject.getObjectName(), new StringDatatype(
//                        StringDatatype.createClassAndVersion(),
//                        StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII),
//                        hdfDataObject.getObjectName().length(),
//                        dt.getDataFile())
//                ));
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
    public HdfDataHolder getData(HdfDataFile dataFile) {
        return hdfDataHolder;
    }
}
