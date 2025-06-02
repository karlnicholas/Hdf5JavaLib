package org.hdf5javalib.redo.reference;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.datatype.FixedPointDatatype;
import org.hdf5javalib.redo.datatype.ReferenceDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class HdfDatasetRegionReference implements HdfReferenceInstance {
    private final boolean external;
    public HdfDatasetRegionReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
        FixedPointDatatype offsetSpec = dt.getDataFile().getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset();
        int offsetSize = offsetSpec.getSize();
        HdfFixedPoint heapOffset = new HdfFixedPoint(Arrays.copyOfRange(bytes, 0, offsetSize) , offsetSpec);
        ByteBuffer bb = ByteBuffer.wrap(bytes, offsetSize, bytes.length - offsetSize).order(ByteOrder.LITTLE_ENDIAN);
        byte[] dataBytes = dt.getDataFile().getGlobalHeap().getDataBytes(heapOffset, bb.getInt());
        System.out.println("byte[] dataBytes: " + Arrays.toString(dataBytes));
    }

    @Override
    public String toString() {
        return "HdfDatasetRegionReference [external=" + external + "]";
    }
}
