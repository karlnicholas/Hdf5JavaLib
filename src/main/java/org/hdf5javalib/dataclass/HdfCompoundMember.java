package org.hdf5javalib.dataclass;

import lombok.Getter;
import lombok.ToString;

import java.nio.ByteBuffer;

@Getter
@ToString
public class HdfCompoundMember implements HdfData{
    private final String name;
    private final HdfData data;

    public HdfCompoundMember(String name, HdfData data) {
        this.name = name;
        this.data = data;
    }

    @Override
    public short getSizeMessageData() {
        return data.getSizeMessageData();
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        data.writeValueToByteBuffer(buffer);
    }
}
