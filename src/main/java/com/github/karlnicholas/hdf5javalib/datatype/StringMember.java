package com.github.karlnicholas.hdf5javalib.datatype;

import com.github.karlnicholas.hdf5javalib.data.HdfString;
import lombok.Getter;

import java.nio.ByteBuffer;

@Getter
public class StringMember implements HdfDataType {
    private final byte version;
    private final short size;
    private final int paddingType;
    private final String paddingDescription;
    private final int charSet;
    private final String charSetDescription;
    private final short sizeMessageData;

    public StringMember(byte version, short size, int paddingType, String paddingDescription, int charSet, String charSetDescription, short sizeMessageData) {
        this.version = version;
        this.size = size;
        this.paddingType = paddingType;
        this.paddingDescription = paddingDescription;
        this.charSet = charSet;
        this.charSetDescription = charSetDescription;
        this.sizeMessageData = sizeMessageData;
    }

    public HdfString getInstance(ByteBuffer dataBuffer) {
        byte[] bytes = new byte[(int)size];
        dataBuffer.get(bytes);
        return new HdfString(bytes, paddingType > 0 , charSet > 0 );
    }

    @Override
    public String toString() {
        return "StringMember{" +
                "size=" + size +
                ", paddingType=" + paddingType +
                ", paddingDescription='" + paddingDescription + '\'' +
                ", charSet=" + charSet +
                ", charSetDescription='" + charSetDescription + '\'' +
                '}';
    }

    @Override
    public short getSizeMessageData() {
        return sizeMessageData;
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // class and version
        buffer.put((byte)(version << 4 | 0b11));
        byte[] classBits = new byte[3];
        buffer.put(classBits);
        buffer.putInt(size);
    }

}

