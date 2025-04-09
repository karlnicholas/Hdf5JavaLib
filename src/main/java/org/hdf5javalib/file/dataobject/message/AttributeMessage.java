package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Represents an Attribute Message in the HDF5 file format.
 *
 * <p>The Attribute Message stores metadata about an HDF5 object, such as a dataset,
 * group, or named datatype. Attributes provide additional descriptive information,
 * such as units, labels, or other user-defined metadata, without affecting the
 * dataset's primary data.</p>
 *
 * <h2>Structure</h2>
 * <p>The Attribute Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the attribute message format.</li>
 *   <li><b>Name (variable-length)</b>: Specifies the name of the attribute.</li>
 *   <li><b>Datatype Message</b>: Defines the datatype of the attribute's value.</li>
 *   <li><b>Dataspace Message</b>: Defines the dimensionality (rank) and size of the attribute's value.</li>
 *   <li><b>Raw Data (variable-length)</b>: Stores the actual attribute value(s).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>Attribute Messages are used for:</p>
 * <ul>
 *   <li>Adding metadata to datasets, groups, or named datatypes.</li>
 *   <li>Storing small amounts of auxiliary data efficiently.</li>
 *   <li>Providing descriptive labels, units, or other contextual information.</li>
 * </ul>
 *
 * <h2>Processing</h2>
 * <p>Attributes in HDF5 are stored similarly to datasets but are not intended for
 * large-scale data storage. They are typically read and written via the attribute
 * interface in HDF5 libraries.</p>
 *
 * <p>This class provides methods to parse and interpret Attribute Messages
 * based on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___a_t_t_r_i_b_u_t_e.html">
 *      HDF5 Attribute Documentation</a>
 */
@Getter
public class AttributeMessage extends HdfMessage {
    private final int version;
    private final HdfString name;
    private final DatatypeMessage datatypeMessage;
    private final DataspaceMessage dataspaceMessage;
    private final HdfData value;

    public AttributeMessage(int version, HdfString name, DatatypeMessage datatypeMessage, DataspaceMessage dataspaceMessage, HdfData value, byte flags, short sizeMessageData) {
        super(MessageType.AttributeMessage, sizeMessageData, flags);
        this.version = version;
        this.datatypeMessage = datatypeMessage;
        this.dataspaceMessage = dataspaceMessage;
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
        int nameSize = Short.toUnsignedInt(buffer.getShort());
        int datatypeSize = Short.toUnsignedInt(buffer.getShort());
        int dataspaceSize = Short.toUnsignedInt(buffer.getShort());

        // Read the name (variable size)
        byte[] nameBytes = new byte[nameSize];
        buffer.get(nameBytes);
        BitSet bitSet = StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.ASCII);
        HdfString name = new HdfString(nameBytes, new StringDatatype(StringDatatype.createClassAndVersion(), bitSet, nameSize));
        // get padding bytes
        int padding = (8 - (nameSize % 8)) % 8;
        byte[] paddingBytes = new byte[padding];
        buffer.get(paddingBytes);

        byte[] dtBytes = new byte[datatypeSize];
        buffer.get(dtBytes);
        // get padding bytes
        padding = (8 - (datatypeSize % 8)) % 8;
        paddingBytes = new byte[padding];
        buffer.get(paddingBytes);

        byte[] dsBytes = new byte[dataspaceSize];
        buffer.get(dsBytes);
        // get padding bytes
        padding = (8 - (dataspaceSize % 8)) % 8;
        paddingBytes = new byte[padding];
        buffer.get(paddingBytes);

        HdfMessage hdfDataObjectHeaderDt = createMessageInstance(MessageType.DatatypeMessage, (byte) 0, dtBytes, offsetSize, lengthSize, ()-> Arrays.copyOfRange( data, buffer.position(), data.length));
        DatatypeMessage dt = (DatatypeMessage) hdfDataObjectHeaderDt;
        HdfMessage hdfDataObjectHeaderDs = createMessageInstance(MessageType.DataspaceMessage, (byte) 0, dsBytes, offsetSize, lengthSize, null);
        DataspaceMessage ds = (DataspaceMessage) hdfDataObjectHeaderDs;

//        HdfString value = null;
//        if ( dt.getHdfDatatype().getDatatypeClass() == HdfDatatype.DatatypeClass.STRING ) {
            int dtDataSize = dt.getHdfDatatype().getSize();
            byte[] dataBytes = new byte[dtDataSize];
            buffer.get(dataBytes);
            HdfData value = dt.getHdfDatatype().getInstance(HdfData.class, dataBytes); // new HdfString(dataBytes, new StringDatatype(StringDatatype.createClassAndVersion(), bitSet, dataBytes.length));
//        }
        return new AttributeMessage(version, name, dt, ds, value, flags, (short)data.length);
    }

    @Override
    public String toString() {
        return "AttributeMessage("+(getSizeMessageData()+8)+"){" +
                "version=" + version +
                ", name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put((byte) version);

        // Skip the reserved byte (1 byte, should be zero)
        buffer.put((byte) 0);

        // Write the sizes of name, datatype, and dataspace (2 bytes each)
        byte[] nameBytes = name.getBytes();
        int nameSize = nameBytes.length;
        buffer.putShort((short) nameSize);
//        buffer.putShort(datatypeMessage.getSizeMessageData());
//        buffer.putShort(dataspaceMessage.getSizeMessageData());
        //TODO: 8 ?
        buffer.putShort((short) 8);
        buffer.putShort((short) 8);

        // Read the name (variable size)
        buffer.put(nameBytes);

        // padding bytes
        byte[] paddingBytes = new byte[(8 - (nameSize % 8)) % 8];
        buffer.put(paddingBytes);

        datatypeMessage.writeInfoToByteBuffer(buffer);

        dataspaceMessage.writeInfoToByteBuffer(buffer);

        // not right
        value.writeValueToByteBuffer(buffer);

    }

//    public void write(HdfFixedPoint attrType, String attributeValue) {
//        value = new HdfString(attributeValue);
//        int size = super.getSizeMessageData() + value.getSizeMessageData();
//        size = (short) ((size + 7) & ~7);
//        super.setSizeMessageData((short) size);
//    }
}
