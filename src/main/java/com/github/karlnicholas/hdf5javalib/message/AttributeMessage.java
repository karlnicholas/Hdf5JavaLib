package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.datatype.HdfDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.createMessageInstance;

@Getter
public class AttributeMessage extends HdfMessage {
    private final int version;
    private final int nameSize;
    private final int datatypeSize;
    private final int dataspaceSize;
    private final HdfMessage dataTypeMessage;
    private final HdfMessage dataSpaceMessage;
    private final HdfString name;
    private final HdfDataType value;

    public AttributeMessage(int version, int nameSize, int datatypeSize, int dataspaceSize, HdfMessage dataTypeMessage, HdfMessage dataSpaceMessage, HdfString name, HdfDataType value) {
        super((short)12, ()-> (short) (1+1+2+2+2+nameSize+(((((nameSize + 1) / 8) + 1) * 8) - nameSize)+datatypeSize+dataspaceSize+value.getSizeMessageData()), (byte)0);
        this.version = version;
        this.nameSize = nameSize;
        this.datatypeSize = datatypeSize;
        this.dataspaceSize = dataspaceSize;
        this.dataTypeMessage = dataTypeMessage;
        this.dataSpaceMessage = dataSpaceMessage;
        this.name = name;
        this.value = value;
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Read the version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());

        // Skip the reserved byte (1 byte, should be zero)
        buffer.get();

        // Read the sizes of name, datatype, and dataspace (2 bytes each)
        int nameSize = nameSize = Short.toUnsignedInt(buffer.getShort());
        int datatypeSize = datatypeSize = Short.toUnsignedInt(buffer.getShort());
        int dataspaceSize = dataspaceSize = Short.toUnsignedInt(buffer.getShort());

        // Read the name (variable size)
        byte[] nameBytes = new byte[nameSize];
        buffer.get(nameBytes);
        HdfString name = new HdfString(nameBytes, true, false);
        // get padding bytes
        int padding = ((((nameSize + 1) / 8) + 1) * 8) - nameSize;
        byte[] paddingBytes = new byte[padding];
        buffer.get(paddingBytes);

        byte[] dtBytes = new byte[datatypeSize];
        buffer.get(dtBytes);

        byte[] dsBytes = new byte[dataspaceSize];
        buffer.get(dsBytes);

        HdfMessage hdfDataObjectHeaderDt = createMessageInstance(3, (byte) 0, dtBytes, offsetSize, lengthSize, ()-> Arrays.copyOfRange( data, buffer.position(), data.length));
        DataTypeMessage dt = (DataTypeMessage) hdfDataObjectHeaderDt;
        HdfMessage hdfDataObjectHeaderDs = createMessageInstance(1, (byte) 0, dsBytes, offsetSize, lengthSize, null);
        DataSpaceMessage ds = (DataSpaceMessage) hdfDataObjectHeaderDs;


//        HdfDataType value = null;
//
//        if ( dt.getDataTypeClass() == 3 ) {
//            int dtDataSize = dt.getSize().getBigIntegerValue().intValue();
//            byte[] dataBytes = new byte[dtDataSize];
//            buffer.get(dataBytes);
//            dt.addDataType(dataBytes);
//            value = dt.getHdfDataType();
//        }
        return new AttributeMessage(version, nameSize, datatypeSize, dataspaceSize, dt, ds, name, dt.getHdfDataType());
    }

    @Override
    public String toString() {
        return "AttributeMessage{" +
                "version=" + version +
                ", nameSize=" + nameSize +
                ", datatypeSize=" + datatypeSize +
                ", dataspaceSize=" + dataspaceSize +
                ", name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put((byte) version);

        // Skip the reserved byte (1 byte, should be zero)
        buffer.put((byte) 0);

        // Read the sizes of name, datatype, and dataspace (2 bytes each)
        buffer.putShort((short) nameSize);
        buffer.putShort((short) datatypeSize);
        buffer.putShort((short) dataspaceSize);

        // Read the name (variable size)
        buffer.put(name.getHdfBytes());
        buffer.put((byte) 0);

        // padding bytes
        buffer.put(new byte[((((nameSize + 1) / 8) + 1) * 8) - nameSize]);

        dataTypeMessage.writeToByteBuffer(buffer);

        dataSpaceMessage.writeToByteBuffer(buffer);

        value.writeToByteBuffer(buffer);


    }
}
