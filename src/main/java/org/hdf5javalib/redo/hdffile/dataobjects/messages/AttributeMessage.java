package org.hdf5javalib.redo.hdffile.dataobjects.messages;

import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfData;
import org.hdf5javalib.redo.dataclass.HdfString;
import org.hdf5javalib.redo.datatype.HdfDatatype;
import org.hdf5javalib.redo.datatype.StringDatatype;
import org.hdf5javalib.redo.utils.HdfDataHolder;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
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
    /**
     * The version of the attribute message format.
     */
    private final int version;
    /**
     * The name of the attribute.
     */
    private final HdfString name;
    /**
     * The datatype of the attribute's value.
     */
    private final DatatypeMessage datatypeMessage;
    /**
     * The dataspace defining the dimensionality and size of the attribute's value.
     */
    private final DataspaceMessage dataspaceMessage;
    /**
     * The actual value of the attribute.
     */
    private final HdfDataHolder hdfDataHolder;

    /**
     * Constructs an AttributeMessage with the specified components.
     *
     * @param version          the version of the attribute message format
     * @param name             the name of the attribute
     * @param datatypeMessage  the datatype of the attribute's value
     * @param dataspaceMessage the dataspace defining the attribute's value dimensions
     * @param hdfDataHolder    the actual attribute value
     * @param flags            message flags
     * @param sizeMessageData  the size of the message data in bytes
     */
    public AttributeMessage(int version, HdfString name, DatatypeMessage datatypeMessage, DataspaceMessage dataspaceMessage, HdfDataHolder hdfDataHolder, int flags, int sizeMessageData) {
        super(MessageType.AttributeMessage, sizeMessageData, flags);
        this.version = version;
        this.datatypeMessage = datatypeMessage;
        this.dataspaceMessage = dataspaceMessage;
        this.name = name;
        this.hdfDataHolder = hdfDataHolder;
    }

    /**
     * Parses an AttributeMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for global heap and other resources
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
        HdfString name = new HdfString(nameBytes, new StringDatatype(StringDatatype.createClassAndVersion(), bitSet, nameSize, hdfDataFile));
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
        int dimensionality = ds.getDimensionality();

        // Assuming ds is an HDF5 dataset, dt is its datatype, buffer is a ByteBuffer, dtDataSize is element size
        // Case 1: Scalar data (dimensionality is 0)
        if (dimensionality == 0) {
            byte[] dataBytes = new byte[dtDataSize];
            buffer.get(dataBytes);
            HdfData scalarValue = dt.getHdfDatatype().getInstance(HdfData.class, dataBytes);
            return new AttributeMessage(version, name, dt, ds, HdfDataHolder.ofScalar(scalarValue), flags, (short) data.length);
        }

        // Case 2: Array data (dimensionality is 1 or more)
        int[] dimensions = Arrays.stream(ds.getDimensions()).mapToInt(dim -> dim.getInstance(Long.class).intValue()).toArray();

        // Step 1: Create the n-dimensional array dynamically.
        // Array.newInstance() is the key. It can create an array of any type with any dimensions.
        // Example: Array.newInstance(HdfData.class, 2, 3) creates a HdfData[2][3]
        Object multiDimArray = Array.newInstance(HdfData.class, dimensions);

        // Step 2: Populate the array recursively from the flat buffer.
        populateArray(multiDimArray, dimensions, 0, buffer, dt.getHdfDatatype(), dtDataSize);

        return new AttributeMessage(version, name, dt, ds, HdfDataHolder.ofArray(multiDimArray, dimensions), flags, (short) data.length);

    }

    /**
     * A recursive helper method to populate an n-dimensional array from a flat ByteBuffer.
     * It works by iterating through the dimensions one by one.
     *
     * @param currentArray The array (or sub-array) to populate at the current level of recursion.
     * @param dimensions   The full shape of the top-level array.
     * @param depth        The current dimension index we are processing (0 for the outermost).
     * @param buffer       The single ByteBuffer to read all data from.
     * @param dt           The datatype object.
     * @param dtDataSize   The size of a single element.
     */
    private static void populateArray(Object currentArray, int[] dimensions, int depth, ByteBuffer buffer, HdfDatatype dt, int dtDataSize) {
        // Base Case: We've recursed to the innermost dimension.
        // The 'currentArray' is now a 1D array (HdfData[]) that we can fill directly.
        if (depth == dimensions.length - 1) {
            int size = dimensions[depth];
            for (int i = 0; i < size; i++) {
                byte[] dataBytes = new byte[dtDataSize];
                buffer.get(dataBytes);
                HdfData value = dt.getInstance(HdfData.class, dataBytes);
                Array.set(currentArray, i, value); // Set the value in the 1D array
            }
        } else {
            // Recursive Step: We are in an outer dimension.
            // 'currentArray' is an array of arrays (e.g., HdfData[][]).
            // We need to iterate through it and make a recursive call for each sub-array.
            int size = dimensions[depth];
            for (int i = 0; i < size; i++) {
                Object subArray = Array.get(currentArray, i); // Get the next sub-array
                populateArray(subArray, dimensions, depth + 1, buffer, dt, dtDataSize);
            }
        }
    }

    /**
     * Returns a string representation of this AttributeMessage.
     *
     * @return a string describing the message size, version, name, and value
     */
    @Override
    public String toString() {
        return "AttributeMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", name='" + name + '\'' +
                ", value='" + hdfDataHolder + '\'' +
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

    }

    public HdfDataHolder getHdfDataHolder() {
        return hdfDataHolder;
    }

}