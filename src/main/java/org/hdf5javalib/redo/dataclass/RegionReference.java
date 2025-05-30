package org.hdf5javalib.redo.dataclass;

import org.hdf5javalib.redo.datatype.ReferenceDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

/** Resolved dataset region reference (H5R_DATASET_REGION1 or H5R_DATASET_REGION2). */
public class RegionReference {
    private final HdfReference datasetReference;
    private final byte[] selectionData;

    public RegionReference(HdfReference datasetReference, byte[] selectionData) {
        if (datasetReference == null) throw new IllegalArgumentException("Dataset reference cannot be null");
        if (selectionData == null) throw new IllegalArgumentException("Selection data cannot be null");
        this.datasetReference = datasetReference;
        this.selectionData = selectionData.clone();
    }

    public HdfReference getDatasetReference() {
        return datasetReference;
    }

    public byte[] getSelectionData() {
        return selectionData.clone();
    }

    public static RegionReference fromHeapData(byte[] data, ReferenceDatatype datatype) {
        if (data == null || data.length < 8) throw new IllegalArgumentException("Invalid region reference data: must be at least 8 bytes");
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        byte[] datasetRefBytes = new byte[8];
        buffer.get(datasetRefBytes);
        byte[] selectionData = new byte[data.length - 8];
        if (buffer.hasRemaining()) buffer.get(selectionData);
        ReferenceDatatype datasetDt = createDatasetReferenceDatatype(datatype);
        HdfReference datasetReference = new HdfReference(datasetRefBytes, datasetDt);
        return new RegionReference(datasetReference, selectionData);
    }

    private static ReferenceDatatype createDatasetReferenceDatatype(ReferenceDatatype regionDt) {
        ReferenceDatatype.ReferenceType type = (regionDt.getReferenceType() == ReferenceDatatype.ReferenceType.DATASET_REGION2) ?
                ReferenceDatatype.ReferenceType.OBJECT2 : ReferenceDatatype.ReferenceType.OBJECT1;
        byte classAndVersion = ReferenceDatatype.createClassAndVersion();
        BitSet classBitField = ReferenceDatatype.createClassBitField(type);
        return new ReferenceDatatype(classAndVersion, classBitField, 8);
    }

    @Override
    public String toString() {
        return "RegionReference{dataset=" + datasetReference + ", selectionLength=" + selectionData.length + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegionReference that = (RegionReference) o;
        return Objects.equals(datasetReference, that.datasetReference) &&
                Arrays.equals(selectionData, that.selectionData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetReference, Arrays.hashCode(selectionData));
    }
}