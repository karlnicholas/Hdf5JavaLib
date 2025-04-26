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
     * Constructs an HdfCompoundMember from a byte array and a specified CompoundMemberDatatype.
     * <p>
     * This constructor initializes the HdfCompoundMember by storing a reference to the provided byte array
     * and associating it with the given datatype. The byte array contains data used for lookup in the global
     * heap, as defined by the datatype's specifications. This class is intended for internal use within the
     * HDF library and should not be directly constructed by applications. Instead, applications should
     * utilize the {@link HdfCompound} class to manage compound data.
     * </p>
     *
     * @param bytes    the byte array containing the compound member data for global heap lookup
     * @param datatype the CompoundMemberDatatype defining the member's structure and format
     * @throws NullPointerException if either {@code bytes} or {@code datatype} is null
     */
    public HdfCompoundMember(byte[] bytes, CompoundMemberDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
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
