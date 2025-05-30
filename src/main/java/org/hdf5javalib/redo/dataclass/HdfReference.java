package org.hdf5javalib.redo.dataclass;

import org.hdf5javalib.redo.datatype.ReferenceDatatype;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/** HDF5 reference data structure (Class 7) for objects, dataset regions, or attributes. */
public class HdfReference implements HdfData {
    private final byte[] bytes;
    private final ReferenceDatatype datatype;
    private final RegionReference regionReference;

    public HdfReference(byte[] bytes, ReferenceDatatype datatype) {
        if (bytes == null || bytes.length == 0) throw new IllegalArgumentException("Bytes cannot be null or empty");
        if (datatype == null) throw new IllegalArgumentException("Datatype cannot be null");
        this.bytes = bytes;
        this.datatype = datatype;
        this.regionReference = null;
    }

    public HdfReference(byte[] bytes, ReferenceDatatype datatype, HdfGlobalHeap globalHeap) {
        if (bytes == null || bytes.length == 0) throw new IllegalArgumentException("Bytes cannot be null or empty");
        if (datatype == null) throw new IllegalArgumentException("Datatype cannot be null");
        this.bytes = bytes;
        this.datatype = datatype;
        this.regionReference = resolveRegionReference(bytes, datatype, globalHeap);
    }

    private RegionReference resolveRegionReference(byte[] bytes, ReferenceDatatype datatype, HdfGlobalHeap globalHeap) {
        if (datatype.getReferenceType() != ReferenceDatatype.ReferenceType.DATASET_REGION1 &&
                datatype.getReferenceType() != ReferenceDatatype.ReferenceType.DATASET_REGION2) {
            return null;
        }
        if (bytes.length != 16) throw new IllegalArgumentException("Region reference length must be 16 bytes, got: " + bytes.length);
        if (globalHeap == null) throw new IllegalArgumentException("Global heap required for region references");
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int dataSize = buffer.getInt();
        long heapOffset = buffer.getLong();
        int objectId = buffer.getInt();

        HdfFixedPoint heapOffsetPoint = HdfWriteUtils.hdfFixedPointFromValue(heapOffset, null);
        byte[] heapData = globalHeap.getDataBytes(heapOffsetPoint, objectId);
        return RegionReference.fromHeapData(heapData, datatype);
    }

    public byte[] getBytes() {
        return bytes.clone();
    }

    public ReferenceDatatype getDatatype() {
        return datatype;
    }

    public RegionReference getRegionReference() {
        return regionReference;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02X", b));
        String base = "Reference[" + datatype.getReferenceType().getDescription() + "]=" + sb;
        if (regionReference != null) base += ", region=" + regionReference;
        return base;
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HdfReference that = (HdfReference) o;
        return Arrays.equals(bytes, that.bytes) &&
                Objects.equals(datatype, that.datatype) &&
                Objects.equals(regionReference, that.regionReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bytes), datatype, regionReference);
    }
}