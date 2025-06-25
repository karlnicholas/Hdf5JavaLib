package org.hdf5javalib.maydo.hdffile.dataobjects.messages;

import org.hdf5javalib.maydo.hdfjava.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Represents a Group Info Message (Type 0x000A) in the HDF5 file format.
 * <p>
 * The {@code GroupInfoMessage} stores constant information defining the behavior of a
 * "new style" group. This includes parameters that control how links are stored (compactly
 * vs. densely) and estimations for storage pre-allocation. It works in conjunction with
 * the {@link LinkInfoMessage}, which stores the variable aspects of group links.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte):</b> The version number for this message format (currently 0).</li>
 *   <li><b>Flags (1 byte):</b> A bitfield indicating which optional fields are present.</li>
 *   <li><b>Link Phase Change - Maximum Compact Value (2 bytes, optional):</b> Max number of links to store compactly. Present if flag bit 0 is set.</li>
 *   <li><b>Link Phase Change - Minimum Dense Value (2 bytes, optional):</b> Min number of links to trigger dense storage. Present if flag bit 0 is set.</li>
 *   <li><b>Estimated Number of Entries (2 bytes, optional):</b> Estimated number of links in the group. Present if flag bit 1 is set.</li>
 *   <li><b>Estimated Link Name Length (2 bytes, optional):</b> Estimated length of link names. Present if flag bit 1 is set.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Defines storage strategy thresholds for group links.</li>
 *   <li>Provides hints for pre-allocating space for group entries, improving performance.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 * @see LinkInfoMessage
 */
public class GroupInfoMessage extends HdfMessage {

    public static final byte FLAG_STORE_PHASE_CHANGE_VALUES = 0x01;
    public static final byte FLAG_STORE_ESTIMATED_ENTRIES = 0x02;

    private static final short DEFAULT_ESTIMATED_ENTRIES = 4;
    private static final short DEFAULT_ESTIMATED_LINK_NAME_LENGTH = 8;

    private final byte version;
    private final byte groupInfoFlags;

    // Optional fields, present if flag bit 0 is set
    private final Short maxCompactValue;
    private final Short minDenseValue;

    // Optional fields, present if flag bit 1 is set
    private final Short estimatedNumEntries;
    private final Short estimatedLinkNameLength;

    /**
     * Constructs a GroupInfoMessage with the specified components.
     *
     * @param version                 The message version number.
     * @param groupInfoFlags          The flags specific to this message.
     * @param maxCompactValue         The maximum number of links to store compactly, or null if not present.
     * @param minDenseValue           The minimum number of links for dense storage, or null if not present.
     * @param estimatedNumEntries     The estimated number of entries, or null if not present.
     * @param estimatedLinkNameLength The estimated length of link names, or null if not present.
     * @param messageFlags            The general HDF message flags.
     * @param sizeMessageData         The size of the message data in bytes.
     */
    public GroupInfoMessage(byte version, byte groupInfoFlags, Short maxCompactValue, Short minDenseValue,
                            Short estimatedNumEntries, Short estimatedLinkNameLength,
                            int messageFlags, int sizeMessageData) {
        super(MessageType.GroupInfoMessage, sizeMessageData, messageFlags);
        this.version = version;
        this.groupInfoFlags = groupInfoFlags;
        this.maxCompactValue = maxCompactValue;
        this.minDenseValue = minDenseValue;
        this.estimatedNumEntries = estimatedNumEntries;
        this.estimatedLinkNameLength = estimatedLinkNameLength;
    }

    /**
     * Parses a GroupInfoMessage from the provided data and file context.
     *
     * @param messageFlags The general HDF message flags.
     * @param data         The byte array containing the message data.
     * @param hdfDataFile  The HDF5 file context (not used in this message but required by the interface).
     * @return A new GroupInfoMessage instance.
     */
    public static HdfMessage parseHeaderMessage(int messageFlags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        byte groupInfoFlags = buffer.get();

        boolean storesPhaseChange = (groupInfoFlags & FLAG_STORE_PHASE_CHANGE_VALUES) != 0;
        boolean storesEstimatedEntries = (groupInfoFlags & FLAG_STORE_ESTIMATED_ENTRIES) != 0;

        Short maxCompact = null;
        Short minDense = null;
        if (storesPhaseChange) {
            maxCompact = buffer.getShort();
            minDense = buffer.getShort();
        }

        Short estNumEntries = null;
        Short estLinkNameLength = null;
        if (storesEstimatedEntries) {
            estNumEntries = buffer.getShort();
            estLinkNameLength = buffer.getShort();
        }

        return new GroupInfoMessage(version, groupInfoFlags, maxCompact, minDense,
                estNumEntries, estLinkNameLength,
                messageFlags, data.length);
    }

    /**
     * Writes the GroupInfoMessage data to the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer to write the message data to.
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        buffer.put(this.version);
        buffer.put(this.groupInfoFlags);

        if (storesPhaseChangeValues()) {
            buffer.putShort(Objects.requireNonNull(this.maxCompactValue));
            buffer.putShort(Objects.requireNonNull(this.minDenseValue));
        }

        if (storesEstimatedEntryInfo()) {
            buffer.putShort(Objects.requireNonNull(this.estimatedNumEntries));
            buffer.putShort(Objects.requireNonNull(this.estimatedLinkNameLength));
        }
    }

    // --- Getters and helpers ---

    public byte getVersion() {
        return version;
    }

    public byte getGroupInfoFlags() {
        return groupInfoFlags;
    }

    public boolean storesPhaseChangeValues() {
        return (groupInfoFlags & FLAG_STORE_PHASE_CHANGE_VALUES) != 0;
    }

    public boolean storesEstimatedEntryInfo() {
        return (groupInfoFlags & FLAG_STORE_ESTIMATED_ENTRIES) != 0;
    }

    public Short getMaxCompactValue() {
        return maxCompactValue;
    }

    public Short getMinDenseValue() {
        return minDenseValue;
    }

    /**
     * Returns the estimated number of entries. If not stored in the message,
     * returns the default value of 4.
     * @return The effective estimated number of entries.
     */
    public short getEffectiveEstimatedNumEntries() {
        return estimatedNumEntries != null ? estimatedNumEntries : DEFAULT_ESTIMATED_ENTRIES;
    }

    /**
     * Returns the estimated length of link names. If not stored in the message,
     * returns the default value of 8.
     * @return The effective estimated link name length.
     */
    public short getEffectiveEstimatedLinkNameLength() {
        return estimatedLinkNameLength != null ? estimatedLinkNameLength : DEFAULT_ESTIMATED_LINK_NAME_LENGTH;
    }

    /**
     * Returns a string representation of this GroupInfoMessage.
     *
     * @return A string describing the message's fields.
     */
    @Override
    public String toString() {
        return "GroupInfoMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", groupInfoFlags=" + String.format("0x%02X", groupInfoFlags) +
                ", maxCompactValue=" + (maxCompactValue != null ? maxCompactValue : "N/A") +
                ", minDenseValue=" + (minDenseValue != null ? minDenseValue : "N/A") +
                ", estimatedNumEntries=" + (estimatedNumEntries != null ? estimatedNumEntries : "N/A (Default: 4)") +
                ", estimatedLinkNameLength=" + (estimatedLinkNameLength != null ? estimatedLinkNameLength : "N/A (Default: 8)") +
                '}';
    }
}