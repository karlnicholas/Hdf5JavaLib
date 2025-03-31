package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype.parseFixedPointType;
import static org.hdf5javalib.file.dataobject.message.datatype.FloatingPointDatatype.parseFloatingPointType;
import static org.hdf5javalib.file.dataobject.message.datatype.StringDatatype.parseStringType;
import static org.hdf5javalib.file.dataobject.message.datatype.TimeDatatype.parseTimeType;
import static org.hdf5javalib.file.dataobject.message.datatype.VariableLengthDatatype.parseVariableLengthDatatype;


/**
 * Represents a Datatype Message in the HDF5 file format.
 *
 * <p>The Datatype Message describes the type of data stored in a dataset, including
 * information such as class type, size, byte order, sign representation, and other
 * properties. This message is essential for interpreting the stored data correctly.</p>
 *
 * <h2>Structure</h2>
 * <p>The Datatype Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the datatype format.</li>
 *   <li><b>Class and Bit Field (1 byte)</b>: Defines the datatype class
 *       (e.g., fixed-point, floating-point, string, compound, array, etc.) and
 *       specific properties encoded in bit flags.</li>
 *   <li><b>Size (4 bytes)</b>: Specifies the size of the datatype in bytes.</li>
 *   <li><b>Additional Fields</b>: Depending on the datatype class, extra
 *       information may be included, such as:
 *       <ul>
 *          <li>Byte order and sign representation for fixed-point numbers.</li>
 *          <li>Exponent and mantissa sizes for floating-point numbers.</li>
 *          <li>Character encoding and padding type for strings.</li>
 *          <li>Member details for compound types.</li>
 *          <li>Base datatype and dimensions for array types.</li>
 *       </ul>
 *   </li>
 * </ul>
 *
 * <h2>Datatype Classes</h2>
 * <p>HDF5 supports multiple datatype classes, including:</p>
 * <ul>
 *   <li><b>Fixed-Point</b>: Integer types with signed or unsigned representation.</li>
 *   <li><b>Floating-Point</b>: IEEE 754-compliant floating-point numbers.</li>
 *   <li><b>String</b>: ASCII or UTF-8 encoded text with different padding options.</li>
 *   <li><b>Compound</b>: User-defined structures with multiple named fields.</li>
 *   <li><b>Array</b>: Multidimensional arrays of a base datatype.</li>
 *   <li><b>Opaque</b>: Raw binary data with user-defined interpretation.</li>
 *   <li><b>Variable-Length</b>: Data structures supporting variable-length elements.</li>
 * </ul>
 *
 * <p>This class provides methods to parse and interpret Datatype Messages based
 * on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___d_a_t_a_t_y_p_e.html">
 *      HDF5 Datatype Documentation</a>
 */
@Getter
public class DatatypeMessage extends HdfMessage {
    private final HdfDatatype hdfDatatype;                 // Remaining raw data

    // Constructor to initialize all fields
    public DatatypeMessage(HdfDatatype hdfDatatype, byte flags, short sizeMessageData) {
        super(MessageType.DatatypeMessage, sizeMessageData,flags);
        this.hdfDatatype = hdfDatatype;
    }

//    /**
//     * Parses the header message and returns a constructed instance.
//     *
//     * @param flags      Flags associated with the message (not used here).
//     * @param data       Byte array containing the header message data.
//     * @param ignoredoffsetSize Size of offsets in bytes (not used here).
//     * @param ignoredlengthSize Size of lengths in bytes (not used here).
//     * @return A fully constructed `DatatypeMessage` instance.
//     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int ignoredoffsetSize, int ignoredlengthSize, byte[] dataTypeData) {
//    public static HdfMessage parseHeaderMessage(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        HdfDatatype hdfDatatype = getHdfDatatype(buffer);
        return new DatatypeMessage(hdfDatatype, flags, (short) data.length);
    }

    public static HdfDatatype getHdfDatatype(ByteBuffer buffer) {
        // Parse Version and Datatype Class (packed into a single byte)
        byte classAndVersion = buffer.get();
        byte version = (byte) ((classAndVersion >> 4) & 0x0F); // Top 4 bits
        byte dataTypeClass = (byte) (classAndVersion & 0x0F);  // Bottom 4 bits
        if ( version != 1 ) {
            throw new UnsupportedOperationException("Unsupported version: " + version);
        }

        // Parse Class Bit Field (24 bits)
        byte[] classBits = new byte[3];
        buffer.get(classBits);
        BitSet classBitField = BitSet.valueOf(new long[]{
                ((long) classBits[2] & 0xFF) << 16 | ((long) classBits[1] & 0xFF) << 8 | ((long) classBits[0] & 0xFF)
        });

        // Parse Size (unsigned 4 bytes)
        int size = buffer.getInt();
        return parseMessageDataType(classAndVersion, classBitField, size, buffer);
    }

    private static HdfDatatype parseMessageDataType(byte classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        HdfDatatype.DatatypeClass dataTypeClass = HdfDatatype.DatatypeClass.fromValue(classAndVersion & 0x0F);
        return switch (dataTypeClass) {
            case FIXED -> parseFixedPointType(classAndVersion, classBitField, size, buffer);
            case FLOAT -> parseFloatingPointType(classAndVersion, classBitField, size, buffer);
            case TIME -> parseTimeType(classAndVersion, classBitField, size, buffer);
            case STRING -> parseStringType(classAndVersion, classBitField, size, buffer);
            case COMPOUND -> new CompoundDatatype(classAndVersion, classBitField, size, buffer);
            case VLEN -> parseVariableLengthDatatype(classAndVersion, classBitField, size, buffer);
            default -> throw new UnsupportedOperationException("Unsupported datatype class: " + dataTypeClass);
        };
    }

    @Override
    public String toString() {
        return "DatatypeMessage("+(getSizeMessageData()+8)+"){hdfDatatype=" + hdfDatatype + '}';
    }

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);

        // datatype specifics
        writeInfoToByteBuffer(buffer);
    }

    public void writeInfoToByteBuffer(ByteBuffer buffer) {
        writeDatatypeProperties(buffer, hdfDatatype);
    }

    public static void writeDatatypeProperties(ByteBuffer buffer, HdfDatatype hdfDatatype) {
        // needed for compoundtype but duplicate when fixedpoint type
        // Datatype general information
        buffer.put(hdfDatatype.getClassAndVersion());    // 1
        // Parse Class Bit Field (24 bits)
        byte[] bytes = hdfDatatype.getClassBitField().toByteArray();
        byte[] result = new byte[3];
        // Copy bytes
        System.arraycopy(bytes, 0, result, 0, Math.min(bytes.length, 3));
        buffer.put(result);         // 3
        buffer.putInt(hdfDatatype.getSize());        // 4

        hdfDatatype.writeDefinitionToByteBuffer(buffer);
    }
}
