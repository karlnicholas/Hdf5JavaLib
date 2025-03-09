package org.hdf5javalib.dataclass;

import lombok.Getter;
import lombok.ToString;
import org.hdf5javalib.file.dataobject.message.datatype.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@ToString
public class HdfCompound implements HdfData {
    private final CompoundDatatype datatype;
    private final byte[] bytes;
    private final List<HdfCompoundMember> members;

    public HdfCompound(byte[] bytes, CompoundDatatype datatype) {
        this.datatype = datatype;
        this.bytes = bytes;
        Map<String, CompoundMemberDatatype> nameToMemberMap = datatype.getMembers().stream().collect(Collectors.toMap(CompoundMemberDatatype::getName, compoundMember -> compoundMember));

        members = new ArrayList<>();
        nameToMemberMap.forEach((name, member) -> {
            HdfCompoundMember hdfMember = new HdfCompoundMember(Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset()+member.getSize()), member);
            members.add(hdfMember);
        });
    }

    @Override
    public int getSizeMessageData() {
        return members.stream().mapToInt(HdfCompoundMember::getSizeMessageData).sum();
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
