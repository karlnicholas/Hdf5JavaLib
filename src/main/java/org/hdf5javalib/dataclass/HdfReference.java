package org.hdf5javalib.dataclass;

import org.hdf5javalib.datatype.ReferenceDatatype;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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

    public byte[] getBytes() {
        return bytes.clone();
    }

    public ReferenceDatatype getDatatype() {
        return datatype;
    }

    @Override
    public String toString() {
        try {
            return datatype.toString(bytes);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public <T> T getInstance(Class<T> clazz) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
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