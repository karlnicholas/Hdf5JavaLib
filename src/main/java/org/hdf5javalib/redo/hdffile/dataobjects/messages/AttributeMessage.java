package org.hdf5javalib.redo.hdffile.dataobjects.messages;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfData;
import org.hdf5javalib.redo.dataclass.HdfString;
import org.hdf5javalib.redo.dataclass.HdfVariableLength;
import org.hdf5javalib.redo.datatype.StringDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Represents an Attribute Message in the HDF5 file format.
 * <p>
 * The {@code AttributeMessage} class stores metadata about an HDF5 object, such as a dataset,
 * group, or named datatype. Attributes provide additional descriptive information, such as units,
 * labels, or other user-defined metadata, without affecting the primary data of the dataset.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the attribute message format.</li>
 *   <li><b>Name (variable-length)</b>: The name of the attribute.</li>
 *   <li><b>Datatype Message</b>: The datatype of the attribute's value.</li>
 *   <li><b>Dataspace Message</b>: The dimensionality (rank) and size of the attribute's value.</li>
 *   <li><b>Raw Data (variable-length)</b>: The actual attribute value(s).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Adding metadata to datasets, groups, or named datatypes.</li>
 *   <li>Storing small amounts of auxiliary data efficiently.</li>
 *   <li>Providing descriptive labels, units, or other contextual information.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see DatatypeMessage
 * @see DataspaceMessage
 */
public class AttributeMessage extends HdfMessage {
    /** The version of the attribute message format. */
    private final int version;
    /** The name of the attribute. */
    private final HdfString name;
    /** The datatype of the attribute's value. */
    private final DatatypeMessage datatypeMessage;
    /** The dataspace defining the dimensionality and size of the attribute's value. */
    private final DataspaceMessage dataspaceMessage;
    /** The actual value of the attribute. */
    private HdfData value;

    /**
     * Constructs an AttributeMessage with the specified components.
     *
     * @param version           the version of the attribute message format
     * @param name              the name of the attribute
     * @param datatypeMessage   the datatype of the attribute's value
     * @param dataspaceMessage  the dataspace defining the attribute's value dimensions
     * @param value             the actual attribute value
     * @param flags             message flags
     * @param sizeMessageData   the size of the message data in bytes
     */
    public AttributeMessage(int version, HdfString name, DatatypeMessage datatypeMessage, DataspaceMessage dataspaceMessage, HdfData value, int flags, int sizeMessageData) {
        super(MessageType.AttributeMessage, sizeMessageData, flags);
        this.version = version;
        this.datatypeMessage = datatypeMessage;
        this.dataspaceMessage = dataspaceMessage;
        this.name = name;
        this.value = value;
    }

    /**
     * Parses an AttributeMessage from the provided data and file context.
     *
     * @param flags         message flags
     * @param data          the byte array containing the message data
     * @param hdfDataFile   the HDF5 file context for global heap and other resources
     * @return a new AttributeMessage instance parsed from the data
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
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
        HdfString name = new HdfString(nameBytes, new StringDatatype(StringDatatype.createClassAndVersion(), bitSet, nameSize,hdfDataFile ));
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

        HdfMessage hdfDataObjectHeaderDt = createMessageInstance(MessageType.DatatypeMessage, (byte) 0, dtBytes, hdfDataFile);
        DatatypeMessage dt = (DatatypeMessage) hdfDataObjectHeaderDt;
        HdfMessage hdfDataObjectHeaderDs = createMessageInstance(MessageType.DataspaceMessage, (byte) 0, dsBytes, hdfDataFile);
        DataspaceMessage ds = (DataspaceMessage) hdfDataObjectHeaderDs;

        int dtDataSize = dt.getHdfDatatype().getSize();
        dt.getHdfDatatype().setGlobalHeap(hdfDataFile.getGlobalHeap());
        byte[] dataBytes = new byte[dtDataSize];
        buffer.get(dataBytes);
        HdfData value = dt.getHdfDatatype().getInstance(HdfData.class, dataBytes);

        return new AttributeMessage(version, name, dt, ds, value, flags, (short)data.length);
    }

    /**
     * Returns a string representation of this AttributeMessage.
     *
     * @return a string describing the message size, version, name, and value
     */
    @Override
    public String toString() {
        return "AttributeMessage("+(getSizeMessageData()+8)+"){" +
                "version=" + version +
                ", name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    /**
     * Writes the attribute message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
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
        buffer.putShort((short) datatypeMessage.getSizeMessageData());
        buffer.putShort((short) dataspaceMessage.getSizeMessageData());

        // Write the name (variable size)
        buffer.put(nameBytes);

        // Padding bytes
        byte[] paddingBytes = new byte[(8 - (nameSize % 8)) % 8];
        buffer.put(paddingBytes);

        datatypeMessage.writeInfoToByteBuffer(buffer);
        // Pad to 8-byte boundary
        int position = buffer.position();
        buffer.position((position + 7) & ~7);

        dataspaceMessage.writeInfoToByteBuffer(buffer);

        // Pad to 8-byte boundary
        position = buffer.position();
        buffer.position((position + 7) & ~7);

        if ( value instanceof HdfVariableLength) {
            value.writeValueToByteBuffer(buffer);
        } else if ( value instanceof HdfString){
            value.writeValueToByteBuffer(buffer);
        } else {
            throw new RuntimeException("Unsupported datatype");
        }
    }

    public void setValue(HdfData value) {
        this.value = value;
    }
}