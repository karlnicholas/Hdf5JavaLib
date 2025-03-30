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

    public HdfCompound(byte[] bytes, CompoundDatatype datatype) {
        this.datatype = datatype;
        this.bytes = bytes;
        members = new ArrayList<>();
        datatype.getMembers().forEach(member -> {
            HdfCompoundMember hdfMember = new HdfCompoundMember(Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset()+member.getSize()), member);
            members.add(hdfMember);
        });
    }

    @Override
    public String toString() {
        return members.stream().map(m->m.getDatatype().getName() + "=" + m.getInstance(String.class)).toList().toString();
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
