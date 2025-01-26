package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.message.*;

import java.util.Arrays;

public class HdfDataObjectHeader {
    private final int type;
    private final int size;
    private final byte flags;
    private final byte[] data;
    private final HdfMessage parsedMessage;

    public HdfDataObjectHeader(int type, int size, byte flags, byte[] data, int offsetSize, int lengthSize) {
        this.type = type;
        this.size = size;
        this.flags = flags;
        this.data = data;

        // Instantiate and parse the appropriate message class
        this.parsedMessage = createMessageInstance(type, flags, data, offsetSize, lengthSize);
    }

    private HdfMessage createMessageInstance(int type, byte flags, byte[] data, int offsetSize, int lengthSize) {
        switch (type) {
            case 0:
                return new NullMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 1:
                return new DataSpaceMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 3:
                return new DataTypeMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 5:
                return new FillValueMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 8:
                return new DataLayoutMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 12:
                return new AttributeMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 18:
                return new ObjectModificationTimeMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 16: // Continuation message
                return new ContinuationMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 17:
                return new HdfSymbolTableMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case 19: // Continuation message
                return new BTreeKValuesMessage().parseHeaderMessage(flags, data, offsetSize, lengthSize);
            default:
                throw new IllegalArgumentException("Unknown message type: " + type);
        }
    }

    public int getType() {
        return type;
    }

    public int getSize() {
        return size;
    }

    public byte getFlags() {
        return flags;
    }

    public byte[] getData() {
        return data;
    }

    public HdfMessage getParsedMessage() {
        return parsedMessage;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HdfDataObjectHeader{");
        sb.append("type=").append(type);
        sb.append(", size=").append(size);
        sb.append(", parsedMessage=").append(parsedMessage != null ? parsedMessage.toString() : "Raw Data: " + Arrays.toString(data));
        sb.append('}');
        return sb.toString();
    }
}
