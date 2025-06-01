package org.hdf5javalib.redo.hdffile.dataobjects.messages;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.datatype.CompoundDatatype;
import org.hdf5javalib.redo.datatype.HdfDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.redo.datatype.ArrayDatatype.parseArrayDatatype;
import static org.hdf5javalib.redo.datatype.BitFieldDatatype.parseBitFieldType;
import static org.hdf5javalib.redo.datatype.EnumDatatype.parseEnumDatatype;
import static org.hdf5javalib.redo.datatype.FixedPointDatatype.parseFixedPointType;
import static org.hdf5javalib.redo.datatype.FloatingPointDatatype.parseFloatingPointType;
import static org.hdf5javalib.redo.datatype.OpaqueDatatype.parseOpaqueDatatype;
import static org.hdf5javalib.redo.datatype.ReferenceDatatype.parseReferenceDatatype;
import static org.hdf5javalib.redo.datatype.StringDatatype.parseStringType;
import static org.hdf5javalib.redo.datatype.TimeDatatype.parseTimeType;
import static org.hdf5javalib.redo.datatype.VariableLengthDatatype.parseVariableLengthDatatype;

/**
 * Represents a Datatype Message in the HDF5 file format.
 * <p>
 * The {@code DatatypeMessage} class describes the type of data stored in a dataset or attribute,
 * including information such as class type, size, byte order, sign representation, and other
 * properties. This message is essential for interpreting the stored data correctly.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the datatype format.</li>
 *   <li><b>Class and Bit Field (1 byte)</b>: The datatype class (e.g., fixed-point, floating-point,
 *       string, compound, array) and specific properties encoded in bit flags.</li>
 *   <li><b>Size (4 bytes)</b>: The size of the datatype in bytes.</li>
 *   <li><b>Additional Fields</b>: Varies by datatype class, such as:
 *       <ul>
 *         <li>Byte order and sign for fixed-point numbers.</li>
 *         <li>Exponent and mantissa sizes for floating-point numbers.</li>
 *         <li>Character encoding and padding for strings.</li>
 *         <li>Member details for compound types.</li>
 *         <li>Base datatype and dimensions for array types.</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h2>Datatype Classes</h2>
 * <ul>
 *   <li><b>Fixed-Point</b>: Integer types with signed or unsigned representation.</li>
 *   <li><b>Floating-Point</b>: IEEE 754-compliant floating-point numbers.</li>
 *   <li><b>String</b>: ASCII or UTF-8 encoded text with padding options.</li>
 *   <li><b>Compound</b>: User-defined structures with multiple named fields.</li>
 *   <li><b>Array</b>: Multidimensional arrays of a base datatype.</li>
 *   <li><b>Opaque</b>: Raw binary data with user-defined interpretation.</li>
 *   <li><b>Variable-Length</b>: Data structures supporting variable-length elements.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDatatype
 */
public class DatatypeMessage extends HdfMessage {
    /** The HDF5 datatype describing the data. */
    private final HdfDatatype hdfDatatype;

    /**
     * Constructs a DatatypeMessage with the specified components.
     *
     * @param hdfDatatype     the HDF5 datatype describing the data
     * @param flags           message flags
     * @param sizeMessageData the size of the message data in bytes
     */
    public DatatypeMessage(HdfDatatype hdfDatatype, int flags, short sizeMessageData) {
        super(MessageType.DatatypeMessage, sizeMessageData, flags);
        this.hdfDatatype = hdfDatatype;
    }

    /**
     * Parses a DatatypeMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for datatype resources
     * @return a new DatatypeMessage instance parsed from the data
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        HdfDatatype hdfDatatype = getHdfDatatype(buffer, hdfDataFile);
        return new DatatypeMessage(hdfDatatype, flags, (short) data.length);
    }

    /**
     * Extracts an HdfDatatype from the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer containing the datatype definition
     * @return the parsed HdfDatatype
     */
    public static HdfDatatype getHdfDatatype(ByteBuffer buffer, HdfDataFile hdfDataFile) {
        // Parse Version and Datatype Class (packed into a single byte)
        int classAndVersion = Byte.toUnsignedInt(buffer.get());
        byte[] classBits = new byte[3];
        buffer.get(classBits);
        BitSet classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        // Parse Size (unsigned 4 bytes)
        int size = buffer.getInt();
        return parseMessageDataType(classAndVersion, classBitField, size, buffer, hdfDataFile);
    }

    private static HdfDatatype parseMessageDataType(int classAndVersion, BitSet classBitField, int size, ByteBuffer buffer, HdfDataFile hdfDataFile) {
        HdfDatatype.DatatypeClass dataTypeClass = HdfDatatype.DatatypeClass.fromValue(classAndVersion & 0x0F);
        return switch (dataTypeClass) {
            case FIXED -> parseFixedPointType(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case FLOAT -> parseFloatingPointType(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case TIME -> parseTimeType(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case STRING -> parseStringType(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case BITFIELD -> parseBitFieldType(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case OPAQUE -> parseOpaqueDatatype(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case COMPOUND -> new CompoundDatatype(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case REFERENCE -> parseReferenceDatatype(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case ENUM -> parseEnumDatatype(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case VLEN -> parseVariableLengthDatatype(classAndVersion, classBitField, size, buffer, hdfDataFile);
            case ARRAY -> parseArrayDatatype(classAndVersion, classBitField, size, buffer, hdfDataFile);
        };
    }

    /**
     * Returns a string representation of this DatatypeMessage.
     *
     * @return a string describing the message size and datatype
     */
    @Override
    public String toString() {
        return "DatatypeMessage("+(getSizeMessageData()+8)+"){hdfDatatype=" + hdfDatatype + '}';
    }

    /**
     * Writes the datatype message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        writeInfoToByteBuffer(buffer);
    }

    /**
     * Writes the datatype message information to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the information to
     */
    public void writeInfoToByteBuffer(ByteBuffer buffer) {
        writeDatatypeProperties(buffer, hdfDatatype);
    }

    /**
     * Writes the properties of an HdfDatatype to the provided ByteBuffer.
     *
     * @param buffer     the ByteBuffer to write the datatype properties to
     * @param hdfDatatype the HdfDatatype to write
     */
    public static void writeDatatypeProperties(ByteBuffer buffer, HdfDatatype hdfDatatype) {
        buffer.put((byte) hdfDatatype.getClassAndVersion());    // 1
        // Write Class Bit Field (24 bits)
        byte[] bytes = hdfDatatype.getClassBitField().toByteArray();
        byte[] result = new byte[3];
        // Copy bytes
        System.arraycopy(bytes, 0, result, 0, Math.min(bytes.length, 3));
        buffer.put(result);         // 3
        buffer.putInt(hdfDatatype.getSize());        // 4
        hdfDatatype.writeDefinitionToByteBuffer(buffer);
    }

    public HdfDatatype getHdfDatatype() {
        return hdfDatatype;
    }
}