package org.hdf5javalib.dataclass;

import lombok.Getter;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundMemberDatatype;

import java.nio.ByteBuffer;

/**
 * Holds bytes that are needed for lookup in the global heap
 */
@Getter
public class HdfCompoundMember implements HdfData {
    private final CompoundMemberDatatype datatype;
    private final byte[] bytes;

    /**
     * Holds bytes that are needed for lookup in the global heap
     */
    public HdfCompoundMember(byte[] bytes, CompoundMemberDatatype datatype) {
        this.bytes = bytes;
        this.datatype = datatype;
    }

    @Override
    public String toString() {
        return datatype.getInstance(HdfData.class, bytes).toString();
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(bytes);
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }
}
