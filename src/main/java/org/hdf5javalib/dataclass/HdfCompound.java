package org.hdf5javalib.dataclass;

import lombok.Getter;
import lombok.ToString;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
@ToString
public class HdfCompound implements HdfData {
    private final List<HdfCompoundMember> members;

    public HdfCompound(List<HdfCompoundMember> members) {
        this.members = members;
    }

    @Override
    public short getSizeMessageData() {
        return (short) members.stream().mapToInt(HdfCompoundMember::getSizeMessageData).sum();
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        members.forEach(member -> member.writeValueToByteBuffer(buffer));
    }
}
