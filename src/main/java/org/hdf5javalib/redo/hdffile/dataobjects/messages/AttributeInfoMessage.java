package org.hdf5javalib.redo.hdffile.dataobjects.messages;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents an Attribute Info Message (Type 0x0015) in the HDF5 file format.
 * <p>
 * The {@code AttributeInfoMessage} stores metadata about attributes associated with an HDF5 object.
 * It tracks the creation order of attributes and provides pointers to data structures used for
 * "dense" attribute storage, such as a Fractal Heap and v2 B-trees for indexing.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte):</b> The version number for this message format (currently 0).</li>
 *   <li><b>Flags (1 byte):</b> A bitfield indicating whether attribute creation order is tracked and/or indexed.</li>
 *   <li><b>Maximum Creation Index (2 bytes, optional):</b> The maximum creation index used for attributes on this object. Present if creation order is tracked (flag bit 0 is set).</li>
 *   <li><b>Fractal Heap Address (variable size):</b> Address of the Fractal Heap used for storing dense attributes (as {@link AttributeMessage}s).</li>
 *   <li><b>Attribute Name v2 B-tree Address (variable size):</b> Address of the B-tree for indexing attributes by name.</li>
 *   <li><b>Attribute Creation Order v2 B-tree Address (variable size, optional):</b> Address of the B-tree for indexing attributes by creation order. Present if creation order is indexed (flag bit 1 is set).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Manages metadata for attribute storage on an HDF5 object.</li>
 *   <li>Provides pointers to advanced data structures for efficient handling of a large number of attributes.</li>
 *   <li>Controls whether attribute creation order is preserved and indexed.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 * @see AttributeMessage
 */
public class AttributeInfoMessage extends HdfMessage {

    public static final byte FLAG_TRACK_CREATION_ORDER = 0x01;
    public static final byte FLAG_INDEX_CREATION_ORDER = 0x02;

    private final byte version;
    private final byte attributeInfoFlags;

    // Optional, present if bit 0 of flags is set
    private final Short maximumCreationIndex;

    private final HdfFixedPoint fractalHeapAddress;
    private final HdfFixedPoint attributeNameV2BtreeAddress;

    // Optional, present if bit 1 of flags is set
    private final HdfFixedPoint attributeCreationOrderV2BtreeAddress;

    /**
     * Constructs an AttributeInfoMessage with the specified components.
     *
     * @param version                                The message version number.
     * @param attributeInfoFlags                     The flags specific to this message (creation order tracking/indexing).
     * @param maximumCreationIndex                   The maximum creation index for attributes, or null if not tracked.
     * @param fractalHeapAddress                     The file offset of the Fractal Heap.
     * @param attributeNameV2BtreeAddress            The file offset of the v2 B-tree for name indexing.
     * @param attributeCreationOrderV2BtreeAddress   The file offset of the v2 B-tree for creation order indexing, or null if not indexed.
     * @param messageFlags                           The general HDF message flags.
     * @param sizeMessageData                        The size of the message data in bytes.
     */
    public AttributeInfoMessage(byte version, byte attributeInfoFlags, Short maximumCreationIndex,
                                HdfFixedPoint fractalHeapAddress, HdfFixedPoint attributeNameV2BtreeAddress,
                                HdfFixedPoint attributeCreationOrderV2BtreeAddress,
                                int messageFlags, int sizeMessageData) {
        super(MessageType.AttributeInfoMessage, sizeMessageData, messageFlags);
        this.version = version;
        this.attributeInfoFlags = attributeInfoFlags;
        this.maximumCreationIndex = maximumCreationIndex;
        this.fractalHeapAddress = fractalHeapAddress;
        this.attributeNameV2BtreeAddress = attributeNameV2BtreeAddress;
        this.attributeCreationOrderV2BtreeAddress = attributeCreationOrderV2BtreeAddress;
    }

    /**
     * Parses an AttributeInfoMessage from the provided data and file context.
     *
     * @param messageFlags The general HDF message flags.
     * @param data         The byte array containing the message data.
     * @param hdfDataFile  The HDF5 file context for datatype resources.
     * @return A new AttributeInfoMessage instance.
     */
    public static HdfMessage parseHeaderMessage(int messageFlags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        byte attributeInfoFlags = buffer.get();

        boolean creationOrderTracked = (attributeInfoFlags & FLAG_TRACK_CREATION_ORDER) != 0;
        boolean creationOrderIndexed = (attributeInfoFlags & FLAG_INDEX_CREATION_ORDER) != 0;

        Short maxCreationIndex = null;
        if (creationOrderTracked) {
            // Maximum Creation Index is a 16-bit unsigned integer (2 bytes)
            maxCreationIndex = buffer.getShort();
        }

        HdfFixedPoint fractalHeapAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint nameBtreeAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);

        HdfFixedPoint creationOrderBtreeAddress = null;
        if (creationOrderIndexed) {
            creationOrderBtreeAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        }

        return new AttributeInfoMessage(version, attributeInfoFlags, maxCreationIndex,
                fractalHeapAddress, nameBtreeAddress, creationOrderBtreeAddress,
                messageFlags, data.length);
    }

    /**
     * Writes the AttributeInfoMessage data to the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer to write the message data to.
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put(this.version);
        buffer.put(this.attributeInfoFlags);

        if (isCreationOrderTracked()) {
            buffer.putShort(Objects.requireNonNull(this.maximumCreationIndex));
        }

        writeFixedPointToBuffer(buffer, this.fractalHeapAddress);
        writeFixedPointToBuffer(buffer, this.attributeNameV2BtreeAddress);

        if (isCreationOrderIndexed()) {
            writeFixedPointToBuffer(buffer, Objects.requireNonNull(this.attributeCreationOrderV2BtreeAddress));
        }
    }

    // --- Getters and helpers ---

    public byte getVersion() {
        return version;
    }

    public byte getAttributeInfoFlags() {
        return attributeInfoFlags;
    }

    public boolean isCreationOrderTracked() {
        return (attributeInfoFlags & FLAG_TRACK_CREATION_ORDER) != 0;
    }

    public boolean isCreationOrderIndexed() {
        return (attributeInfoFlags & FLAG_INDEX_CREATION_ORDER) != 0;
    }

    public Short getMaximumCreationIndex() {
        return maximumCreationIndex;
    }

    public HdfFixedPoint getFractalHeapAddress() {
        return fractalHeapAddress;
    }

    public HdfFixedPoint getAttributeNameV2BtreeAddress() {
        return attributeNameV2BtreeAddress;
    }

    public HdfFixedPoint getAttributeCreationOrderV2BtreeAddress() {
        return attributeCreationOrderV2BtreeAddress;
    }

    /**
     * Returns a string representation of this AttributeInfoMessage.
     *
     * @return A string describing the message's fields.
     */
    @Override
    public String toString() {
        return "AttributeInfoMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", attributeInfoFlags=" + String.format("0x%02X", attributeInfoFlags) +
                ", maximumCreationIndex=" + (maximumCreationIndex != null ? maximumCreationIndex : "N/A") +
                ", fractalHeapAddress=" + fractalHeapAddress.getInstance(Long.class) +
                ", attributeNameV2BtreeAddress=" + attributeNameV2BtreeAddress.getInstance(Long.class) +
                ", attributeCreationOrderV2BtreeAddress=" + (attributeCreationOrderV2BtreeAddress != null ? attributeCreationOrderV2BtreeAddress.getInstance(Long.class) : "N/A") +
                '}';
    }
}