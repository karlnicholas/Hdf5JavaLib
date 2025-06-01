package org.hdf5javalib.redo.reference;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.datatype.ReferenceDatatype;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataObject;
import org.hdf5javalib.redo.hdffile.infrastructure.*;
import org.hdf5javalib.redo.hdffile.metadata.HdfSuperblock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

public class HdfObjectReference implements HdfReferenceInstance {
    private static final Logger log = LoggerFactory.getLogger(HdfObjectReference.class);
    private final boolean external;
    private final HdfDataObject hdfDataObject;
    private final HdfFixedPoint hdfFixedPoint;

    public HdfObjectReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
        AtomicReference<HdfDataObject> localHdfDataObject = new AtomicReference<>();
        HdfFixedPoint localHdfFixedPoint;
        if ( !external) {
            HdfSuperblock superblock = dt.getDataFile().getFileAllocation().getSuperblock();
            ReferenceDatatype.ReferenceType type = ReferenceDatatype.getReferenceType(dt.getClassBitField());
            if ( type ==  ReferenceDatatype.ReferenceType.OBJECT1) {
                localHdfFixedPoint = new HdfFixedPoint(bytes, superblock.getFixedPointDatatypeForOffset());
            } else if ( type == ReferenceDatatype.ReferenceType.OBJECT2) {
                if ( bytes[0] == 0x02) {
                    int size = Byte.toUnsignedInt(bytes[2]);
                    localHdfFixedPoint = new HdfFixedPoint(Arrays.copyOfRange(bytes, 3, 3 + size), superblock.getFixedPointDatatypeForOffset());
                } else {
                    localHdfFixedPoint = null;
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
        } else {
            localHdfFixedPoint = null;
        }
        this.hdfFixedPoint = localHdfFixedPoint;
        this.hdfDataObject = localHdfDataObject.get();
    }

    @Override
    public String toString() {
        return "HdfObjectReference [external=" + external + ", hdfDataObject=" + (hdfDataObject == null ? "ObjectId not decoded" : "ObjectName: " + hdfDataObject.getObjectName()) + "]";

    }
}
