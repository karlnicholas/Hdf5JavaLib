package org.hdf5javalib.redo.reference;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.datatype.ReferenceDatatype;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataObject;
import org.hdf5javalib.redo.hdffile.metadata.HdfSuperblock;

import java.util.Arrays;

public class HdfObjectReference implements HdfReferenceInstance {
    private final boolean external;
    private final HdfDataObject hdfDataObject;
    private final HdfFixedPoint hdfFixedPoint;

    public HdfObjectReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
        HdfFixedPoint hdfFixedPoint = null;
        if ( !external) {
            HdfSuperblock superblock = dt.getDataFile().getFileAllocation().getSuperblock();
            ReferenceDatatype.ReferenceType type = ReferenceDatatype.getReferenceType(dt.getClassBitField());
            if ( type ==  ReferenceDatatype.ReferenceType.OBJECT1) {

            } else if ( type == ReferenceDatatype.ReferenceType.OBJECT2) {
                if ( bytes[0] == 0x02) {
                    int size = Byte.toUnsignedInt(bytes[2]);
                    hdfFixedPoint = new HdfFixedPoint(Arrays.copyOfRange(bytes, 3, 3 + size), superblock.getFixedPointDatatypeForOffset());
                }
            } else {
                throw new IllegalArgumentException("Unsupported reference type: " + dt.getClassBitField());
            }

        }
        this.hdfDataObject = null;
        this.hdfFixedPoint = hdfFixedPoint;
    }

    @Override
    public String toString() {
        return "HdfObjectReference [external=" + external + ", hdfFixedPoint=" + (hdfFixedPoint == null ? "ObjectId not decoded" : "ObjectId: " + hdfFixedPoint.toString()) + "]";

    }
}
