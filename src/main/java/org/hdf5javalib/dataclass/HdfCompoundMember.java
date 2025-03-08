package org.hdf5javalib.dataclass;

import lombok.Getter;
import lombok.ToString;

import java.nio.ByteBuffer;

@Getter
@ToString
public class HdfCompoundMember<T> implements HdfData<T> {
    private final Class<T> clazz;
    private final String name;
    private final HdfData data;

    public HdfCompoundMember(Class<T> clazz, String name, HdfData data) {
        this.clazz = clazz;
        this.name = name;
        this.data = data;
    }

    @Override
    public int getSizeMessageData() {
        return data.getSizeMessageData();
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        data.writeValueToByteBuffer(buffer);
    }

    @Override
    public T getInstance() {
        return null;
    }
}
