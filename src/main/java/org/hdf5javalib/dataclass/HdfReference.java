package org.hdf5javalib.dataclass;

import org.hdf5javalib.datatype.ReferenceDatatype;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * HDF5 reference data structure (Class 7) for objects, dataset regions, or attributes.
 */
public class HdfReference implements HdfData {
    private final byte[] bytes;
    private final ReferenceDatatype datatype;

    public HdfReference(byte[] bytes, ReferenceDatatype datatype) {
        if (bytes == null || bytes.length == 0) throw new IllegalArgumentException("Bytes cannot be null or empty");
        if (datatype == null) throw new IllegalArgumentException("Datatype cannot be null");
        this.bytes = bytes;
        this.datatype = datatype;
    }

    public HdfReference(byte[] bytes, ReferenceDatatype datatype, HdfGlobalHeap globalHeap) {
        if (bytes == null || bytes.length == 0) throw new IllegalArgumentException("Bytes cannot be null or empty");
        if (datatype == null) throw new IllegalArgumentException("Datatype cannot be null");
        this.bytes = bytes;
        this.datatype = datatype;
    }


    public byte[] getBytes() {
        return bytes.clone();
    }

    public ReferenceDatatype getDatatype() {
        return datatype;
    }

    @Override
    public String toString() {
        return datatype.toString(bytes);
//        StringBuilder sb = new StringBuilder();
//        for (byte b : bytes) sb.append(String.format("%02X", b));
//        String base = "Reference[" + ReferenceDatatype.getReferenceType(datatype.getClassBitField()).getDescription() + "]=" + sb;
//        return base;
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
                Objects.equals(datatype, that.datatype);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(bytes), datatype);
    }
}