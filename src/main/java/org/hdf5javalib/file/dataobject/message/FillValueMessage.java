package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Represents a Fill Value Message in the HDF5 file format.
 *
 * <p>The Fill Value Message defines the default value used to initialize
 * unallocated or uninitialized elements of a dataset. This ensures that
 * datasets have a consistent default value when accessed before explicit
 * data is written.</p>
 *
 * <h2>Structure</h2>
 * <p>The Fill Value Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the fill value format.</li>
 *   <li><b>Space Allocation Time (1 byte, version-dependent)</b>: Determines when
 *       space for fill values is allocated. Options include:
 *       <ul>
 *         <li>Early (allocated when the dataset is created).</li>
 *         <li>Late (allocated when data is written).</li>
 *         <li>Incremental (allocated gradually as chunks are written).</li>
 *       </ul>
 *   </li>
 *   <li><b>Fill Value Write Time (1 byte, version-dependent)</b>: Specifies when
 *       the fill value is written (on dataset creation or first data write).</li>
 *   <li><b>Fill Value Defined Flag (1 byte)</b>: Indicates whether a user-defined
 *       fill value is provided.</li>
 *   <li><b>Fill Value Size (variable)</b>: Specifies the size of the fill value in bytes.</li>
 *   <li><b>Fill Value Data (variable, optional)</b>: Contains the actual fill value,
 *       matching the dataset's datatype.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <p>Fill values are used to ensure that uninitialized regions of a dataset have
 * predictable content. They are particularly useful for:</p>
 * <ul>
 *   <li>Defining default values for missing or newly allocated data.</li>
 *   <li>Ensuring consistency when reading uninitialized portions of a dataset.</li>
 *   <li>Improving dataset integrity in applications requiring structured default data.</li>
 * </ul>
 *
 * <p>This class provides methods to parse and interpret Fill Value Messages based
 * on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___f_i_l_l_v_a_l_u_e.html">
 *      HDF5 Fill Value Documentation</a>
 */
@Getter
public class FillValueMessage extends HdfMessage {
    private final int version;              // 1 byte
    private final int spaceAllocationTime;  // 1 byte
    private final int fillValueWriteTime;   // 1 byte
    private final int fillValueDefined;     // 1 byte
    private final int size;       // Size of the Fill Value field (optional, unsigned 4 bytes)
    private final byte[] fillValue;         // Fill Value field (optional)

    // Constructor to initialize all fields
    public FillValueMessage(
            int version,
            int spaceAllocationTime,
            int fillValueWriteTime,
            int fillValueDefined,
            int size,
            byte[] fillValue,
            byte flags
    ) {
        super(MessageType.FillValueMessage, ()-> (short) (8), flags);
        this.version = version;
        this.spaceAllocationTime = spaceAllocationTime;
        this.fillValueWriteTime = fillValueWriteTime;
        this.fillValueDefined = fillValueDefined;
        this.size = size;
        this.fillValue = fillValue;
    }

    /**
     * Parses the header message and returns a constructed instance.
     *
     * @param flags      Flags associated with the message (not used here).
     * @param data       Byte array containing the header message data.
     * @param offsetSize Size of offsets in bytes (not used here).
     * @param lengthSize Size of lengths in bytes (not used here).
     * @return A fully constructed `FillValueMessage` instance.
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse the first 4 bytes
        int version = Byte.toUnsignedInt(buffer.get());
        int spaceAllocationTime = Byte.toUnsignedInt(buffer.get());
        int fillValueWriteTime = Byte.toUnsignedInt(buffer.get());
        int fillValueDefined = Byte.toUnsignedInt(buffer.get());

        // Initialize optional fields
        int size = 0;
        byte[] fillValue = null;

        // Handle Version 2+ behavior and fillValueDefined flag
        if (version >= 2 && fillValueDefined == 1) {
            // Parse Size (unsigned 4 bytes, using HdfFixedPoint)
            size =buffer.getInt();

            // Parse Fill Value
            fillValue = new byte[size];
            buffer.get(fillValue);
        }

        // Return a constructed instance of FillValueMessage
        return new FillValueMessage(version, spaceAllocationTime, fillValueWriteTime, fillValueDefined, size, fillValue, flags);
    }

    @Override
    public String toString() {
        return "FillValueMessage{" +
                "version=" + version +
                ", spaceAllocationTime=" + spaceAllocationTime +
                ", fillValueWriteTime=" + fillValueWriteTime +
                ", fillValueDefined=" + fillValueDefined +
                ", size=" + size +
                ", fillValue=" + (fillValue != null ? fillValue.length + " bytes" : "undefined") +
                '}';
    }

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Parse the first 4 bytes
        buffer.put((byte) version);
        buffer.put((byte) spaceAllocationTime);
        buffer.put((byte) fillValueWriteTime);
        buffer.put((byte) fillValueDefined);

        // Handle Version 2+ behavior and fillValueDefined flag
        if (version >= 2 && fillValueDefined == 1) {
            // Parse Size (unsigned 4 bytes, using HdfFixedPoint)
            buffer.putInt(size);
            buffer.put(fillValue);
        }
    }
}
