package org.hdf5javalib.dataclass;

import lombok.Getter;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
public class HdfCompound implements HdfData {
    private final CompoundDatatype datatype;
    private final byte[] bytes;
    private final List<HdfCompoundMember> members;

    /**
     * Constructs an HdfCompound from a byte array and a specified CompoundDatatype.
     * <p>
     * This constructor initializes the HdfCompound by storing a reference to the provided byte array
     * and associating it with the given datatype. It also populates a list of compound members by
     * extracting subarrays from the byte array based on each member's offset and size as defined in
     * the datatype. Each member is represented as an HdfCompoundMember instance. The byte array is
     * expected to contain compound data formatted according to the datatype's specifications.
     * </p>
     *
     * @param bytes    the byte array containing the compound data
     * @param datatype the CompoundDatatype defining the compound structure, member offsets, and sizes
     * @throws NullPointerException if either {@code bytes} or {@code datatype} is null
     */
    public HdfCompound(byte[] bytes, CompoundDatatype datatype) {
        if (bytes == null || datatype == null) {
            throw new NullPointerException("Bytes and datatype must not be null");
        }
        this.datatype = datatype;
        this.bytes = bytes;
        members = new ArrayList<>();
        datatype.getMembers().forEach(member -> {
            HdfCompoundMember hdfMember = new HdfCompoundMember(
                    Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset() + member.getSize()),
                    member
            );
            members.add(hdfMember);
        });
    }

    @Override
    public String toString() {
        return datatype.getInstance(String.class, bytes);
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        members.forEach(member -> member.writeValueToByteBuffer(buffer));
    }

    @Override
    public <T> T getInstance(Class<T> clazz) {
        return datatype.getInstance(clazz, bytes);
    }

}
