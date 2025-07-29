package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Link Info Message (Type 0x0002) in the HDF5 file format.
 * <p>
 * The {@code LinkInfoMessage} tracks information about the links within a "new style" group.
 * It contains flags and addresses that determine how links are stored and indexed, distinguishing
 * between compact storage and more complex structures like Fractal Heaps and v2 B-trees.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte):</b> The version number for this message format (currently 0).</li>
 *   <li><b>Flags (1 byte):</b> A bitfield indicating whether creation order is tracked and/or indexed.</li>
 *   <li><b>Maximum Creation Index (8 bytes, optional):</b> The maximum creation order index used. Present if creation order is tracked (flag bit 0 is set).</li>
 *   <li><b>Fractal Heap Address (variable size):</b> Address of the Fractal Heap used for storing links.</li>
 *   <li><b>v2 B-tree Address for Name Index (variable size):</b> Address of the B-tree for indexing links by name.</li>
 *   <li><b>v2 B-tree Address for Creation Order Index (variable size, optional):</b> Address of the B-tree for indexing links by creation order. Present if creation order is indexed (flag bit 1 is set).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Manages metadata for link storage in modern HDF5 groups.</li>
 *   <li>Provides pointers to advanced data structures (Fractal Heap, v2 B-trees) for efficient link management.</li>
 *   <li>Controls whether link creation order is preserved and indexed.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class LinkInfoMessage extends HdfMessage {

    public static final byte FLAG_TRACK_CREATION_ORDER = 0x01;
    public static final byte FLAG_INDEX_CREATION_ORDER = 0x02;

    private final byte version;
    private final byte linkInfoFlags;
    private final Long maximumCreationIndex; // Optional, hence nullable Long
    private final HdfFixedPoint fractalHeapAddress;
    private final HdfFixedPoint v2BTreeNameIndexAddress;
    private final HdfFixedPoint v2BTreeCreationOrderIndexAddress; // Optional, hence nullable

    /**
     * Constructs a LinkInfoMessage with the specified components.
     *
     * @param version                          The message version number.
     * @param linkInfoFlags                    The flags specific to this message (creation order tracking/indexing).
     * @param maximumCreationIndex             The maximum creation index, or null if not tracked.
     * @param fractalHeapAddress               The file offset of the Fractal Heap.
     * @param v2BTreeNameIndexAddress          The file offset of the v2 B-tree for name indexing.
     * @param v2BTreeCreationOrderIndexAddress The file offset of the v2 B-tree for creation order indexing, or null if not indexed.
     * @param messageFlags                     The general HDF message flags.
     * @param sizeMessageData                  The size of the message data in bytes.
     */
    public LinkInfoMessage(byte version, byte linkInfoFlags, Long maximumCreationIndex,
                           HdfFixedPoint fractalHeapAddress, HdfFixedPoint v2BTreeNameIndexAddress,
                           HdfFixedPoint v2BTreeCreationOrderIndexAddress,
                           int messageFlags, int sizeMessageData) {
        super(MessageType.LINK_INFO_MESSAGE, sizeMessageData, messageFlags);
        this.version = version;
        this.linkInfoFlags = linkInfoFlags;
        this.maximumCreationIndex = maximumCreationIndex;
        this.fractalHeapAddress = fractalHeapAddress;
        this.v2BTreeNameIndexAddress = v2BTreeNameIndexAddress;
        this.v2BTreeCreationOrderIndexAddress = v2BTreeCreationOrderIndexAddress;
    }

    /**
     * Parses a LinkInfoMessage from the provided data and file context.
     *
     * @param messageFlags The general HDF message flags.
     * @param data         The byte array containing the message data.
     * @param hdfDataFile  The HDF5 file context for datatype resources.
     * @return A new LinkInfoMessage instance.
     */
    public static HdfMessage parseHeaderMessage(int messageFlags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        byte linkInfoFlags = buffer.get();

        boolean creationOrderTracked = (linkInfoFlags & FLAG_TRACK_CREATION_ORDER) != 0;
        boolean creationOrderIndexed = (linkInfoFlags & FLAG_INDEX_CREATION_ORDER) != 0;

        Long maxCreationIndex = null;
        if (creationOrderTracked) {
            maxCreationIndex = buffer.getLong();
        }

        HdfFixedPoint fractalHeapAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint v2BTreeNameIndexAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);

        HdfFixedPoint v2BTreeCreationOrderIndexAddress = null;
        if (creationOrderIndexed) {
            v2BTreeCreationOrderIndexAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        }

        return new LinkInfoMessage(version, linkInfoFlags, maxCreationIndex,
                fractalHeapAddress, v2BTreeNameIndexAddress, v2BTreeCreationOrderIndexAddress,
                messageFlags, data.length);
    }

    /**
     * Writes the LinkInfoMessage data to the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer to write the message data to.
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put(this.version);
        buffer.put(this.linkInfoFlags);

        if (Objects.nonNull(this.maximumCreationIndex)) {
            buffer.putLong(this.maximumCreationIndex);
        }

        writeFixedPointToBuffer(buffer, this.fractalHeapAddress);
        writeFixedPointToBuffer(buffer, this.v2BTreeNameIndexAddress);

        if (Objects.nonNull(this.v2BTreeCreationOrderIndexAddress)) {
            writeFixedPointToBuffer(buffer, this.v2BTreeCreationOrderIndexAddress);
        }
    }

    // --- Getters for message fields ---

    public byte getVersion() {
        return version;
    }

    public byte getLinkInfoFlags() {
        return linkInfoFlags;
    }

    public boolean isCreationOrderTracked() {
        return (linkInfoFlags & FLAG_TRACK_CREATION_ORDER) != 0;
    }

    public boolean isCreationOrderIndexed() {
        return (linkInfoFlags & FLAG_INDEX_CREATION_ORDER) != 0;
    }

    public Long getMaximumCreationIndex() {
        return maximumCreationIndex;
    }

    public HdfFixedPoint getFractalHeapAddress() {
        return fractalHeapAddress;
    }



    public HdfFixedPoint getV2BTreeNameIndexAddress() {
        return v2BTreeNameIndexAddress;
    }

    public HdfFixedPoint getV2BTreeCreationOrderIndexAddress() {
        return v2BTreeCreationOrderIndexAddress;
    }


    /**
     * Returns a string representation of this LinkInfoMessage.
     *
     * @return A string describing the message's fields.
     */
    @Override
    public String toString() {
        return "LinkInfoMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", linkInfoFlags=" + String.format("0x%02X", linkInfoFlags) +
                ", maximumCreationIndex=" + (maximumCreationIndex != null ? maximumCreationIndex : "N/A") +
                ", fractalHeapAddress=" + fractalHeapAddress.getInstance(Long.class) +
                ", v2BTreeNameIndexAddress=" + v2BTreeNameIndexAddress.getInstance(Long.class) +
                ", v2BTreeCreationOrderIndexAddress=" + (v2BTreeCreationOrderIndexAddress != null ? v2BTreeCreationOrderIndexAddress.getInstance(Long.class) : "N/A") +
                '}';
    }
}