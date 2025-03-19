package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Represents a base class for HDF5 Object Header Messages.
 *
 * <p>Each object in an HDF5 file has an object header containing metadata messages that
 * describe the object. These messages include essential information about datasets, groups,
 * and other HDF5 structures. The {@code HdfMessage} class serves as a base for all such
 * message types, encapsulating common properties and functionality.</p>
 *
 * <h2>Structure</h2>
 * <p>An HDF5 Object Header Message consists of the following components:</p>
 * <ul>
 *   <li><b>Message Type (2 bytes)</b>: Identifies the type of message stored.</li>
 *   <li><b>Size of Message Data (2 bytes)</b>: Specifies the number of bytes in the
 *       message data, including padding to align the message to an 8-byte boundary.</li>
 *   <li><b>Message Flags (1 byte)</b>: A bit field that defines specific properties of the message:</li>
 *   <ul>
 *       <li>Bit 0: Message data is constant (e.g., Datatype Messages).</li>
 *       <li>Bit 1: Message is shared and stored in another location.</li>
 *       <li>Bit 2: Message should not be shared.</li>
 *       <li>Bit 3: Fail if message type is unknown and file is writable.</li>
 *       <li>Bit 4: Set bit 5 if message type is unknown and file is modified.</li>
 *       <li>Bit 5: Indicates the object was modified by software unaware of this message.</li>
 *       <li>Bit 6: Message is shareable.</li>
 *       <li>Bit 7: Always fail if message type is unknown (even for read-only access).</li>
 *   </ul>
 *   <li><b>Reserved (3 bytes)</b>: Unused space for alignment.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>The {@code HdfMessage} class provides a foundation for handling various
 * HDF5 metadata messages, enabling structured parsing and serialization.</p>
 *
 * <h2>Usage</h2>
 * <p>Concrete implementations of this class represent specific HDF5 message types such as:</p>
 * <ul>
 *   <li>{@code DataspaceMessage} – Defines dataset dimensions.</li>
 *   <li>{@code DatatypeMessage} – Specifies the type of data stored.</li>
 *   <li>{@code FillValueMessage} – Defines default fill values.</li>
 *   <li>{@code DataLayoutMessage} – Describes the storage layout.</li>
 *   <li>{@code AttributeMessage} – Stores user-defined metadata.</li>
 *   <li>And many more...</li>
 * </ul>
 *
 * <p>This class provides methods for encoding and decoding HDF5 object header messages
 * based on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___o_b_j_e_c_t___h_e_a_d_e_r.html">
 *      HDF5 Object Header Documentation</a>
 */
@Getter
public abstract class HdfMessage {
    private final MessageType messageType;
    @Setter
    private short sizeMessageData;
    private final byte messageFlags;

    protected HdfMessage(MessageType messageType, short sizeMessageData, byte messageFlags) {
        this.messageType = messageType;
        this.sizeMessageData = sizeMessageData;
        this.messageFlags = messageFlags;
    }

    /**
     * Header Message Type #1 (short)
     * Size of Header Message Data #1 (short)
     * Header Message #1 Flags (byte)
     * Reserved (zero) (3 bytes)
     *
     * @param buffer ByteBuffer
     */
    protected void writeMessageData(ByteBuffer buffer) {
        buffer.putShort(messageType.getValue());
        buffer.putShort(sizeMessageData);
        buffer.put(messageFlags);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
    }

    public abstract void writeMessageToByteBuffer(ByteBuffer buffer);

    // Parse header messages
    public static List<HdfMessage> readMessagesFromByteBuffer(FileChannel fileChannel, short objectHeaderSize, short offsetSize, short lengthSize) throws IOException {
        ByteBuffer buffer  = ByteBuffer.allocate(objectHeaderSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();
        List<HdfMessage> messages = new ArrayList<>();

        while (buffer.hasRemaining()) {
            // Header Message Type (2 bytes, little-endian)
            HdfMessage.MessageType type = HdfMessage.MessageType.fromValue(buffer.getShort());
            int size = Short.toUnsignedInt(buffer.getShort());
            byte flags = buffer.get();
            buffer.position(buffer.position() + 3); // Skip 3 reserved bytes

            // Header Message Data
            byte[] messageData = new byte[size];
            buffer.get(messageData);

            // Add the message to the list
            messages.add(createMessageInstance(type, flags, messageData, offsetSize, lengthSize, ()-> Arrays.copyOfRange(messageData, 8, messageData.length)));

        }
        return messages;
    }

    protected static HdfMessage createMessageInstance(HdfMessage.MessageType type, byte flags, byte[] data, short offsetSize, short lengthSize, Supplier<byte[]> getDataTypeData) {
        System.out.println("type:flags:length " + type + " " + flags + " " + data.length);
        return switch (type) {
            case NilMessage -> NilMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case DataspaceMessage -> DataspaceMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case DatatypeMessage -> DatatypeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize, getDataTypeData.get());
            case FillValueMessage -> FillValueMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case DataLayoutMessage -> DataLayoutMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case AttributeMessage -> AttributeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case ObjectHeaderContinuationMessage -> ObjectHeaderContinuationMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case SymbolTableMessage -> SymbolTableMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case ObjectModificationTimeMessage -> ObjectModificationTimeMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            case BtreeKValuesMessage -> BTreeKValuesMessage.parseHeaderMessage(flags, data, offsetSize, lengthSize);
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        };
    }

    // TODO: fix recursion
    public static List<HdfMessage> parseContinuationMessage(FileChannel fileChannel, ObjectHeaderContinuationMessage objectHeaderContinuationMessage, short offsetSize, short lengthSize) throws IOException {
        long continuationOffset = objectHeaderContinuationMessage.getContinuationOffset().getInstance(Long.class);
        short continuationSize = objectHeaderContinuationMessage.getContinuationSize().getInstance(Long.class).shortValue();

        // Move to the continuation block offset
        fileChannel.position(continuationOffset);

        // Parse the continuation block messages
        return new ArrayList<>(readMessagesFromByteBuffer(fileChannel, continuationSize, offsetSize, lengthSize));
    }

    /**
     * Enum representing various HDF5 message types.
     */
    @Getter
    public enum MessageType {
        /**
         * **NIL Message (0x0000)**:
         * A placeholder message that should be ignored when reading object headers.
         */
        NilMessage((short) 0, "NIL Message"),

        /**
         * **Dataspace Message (0x0001)**:
         * Defines the dimensions and size of a dataset, including rank (number of dimensions).
         */
        DataspaceMessage((short) 1, "Dataspace Message"),

        /**
         * **Link Info Message (0x0002)**:
         * Stores metadata about links, such as creation order tracking and link storage type.
         */
        LinkInfoMessage((short) 2, "Link Info Message"),

        /**
         * **Datatype Message (0x0003)**:
         * Specifies the datatype for elements of a dataset, including size and encoding.
         */
        DatatypeMessage((short) 3, "Datatype Message"),

        /**
         * **Data Storage - Fill Value (Old) Message (0x0004)**:
         * Defines default values for uninitialized dataset elements (older version).
         */
        FillMessage((short) 4, "Data Storage - Fill Value (Old) Message"),

        /**
         * **Data Storage - Fill Value Message (0x0005)**:
         * A newer version of the fill value message, supporting extended features.
         */
        FillValueMessage((short) 5, "Data Storage - Fill Value Message"),

        /**
         * **Link Message (0x0006)**:
         * Represents a link to another object in the HDF5 file.
         */
        LinkMessage((short) 6, "Link Message"),

        /**
         * **Data Storage - External Data Files Message (0x0007)**:
         * Indicates that dataset raw data is stored in external files.
         */
        ExternalDataFilesMessage((short) 7, "Data Storage - External Data Files Message"),

        /**
         * **Data Layout Message (0x0008)**:
         * Describes how raw data is stored, including chunked, contiguous, or compact layouts.
         */
        DataLayoutMessage((short) 8, "Data Layout Message"),

        /**
         * **Bogus Message (0x0009)**:
         * Used for internal testing, should not appear in normal HDF5 files.
         */
        BogusMessage((short) 9, "Bogus Message"),

        /**
         * **Group Info Message (0x000A)**:
         * Provides information about a group, such as estimated link count and heap sizes.
         */
        GroupInfoMessage((short) 10, "Group Info Message"),

        /**
         * **Data Storage - Filter Pipeline Message (0x000B)**:
         * Defines compression and other data filters applied to datasets.
         */
        FilterPipelineMessage((short) 11, "Data Storage - Filter Pipeline Message"),

        /**
         * **Attribute Message (0x000C)**:
         * Stores metadata attributes associated with an object.
         */
        AttributeMessage((short) 12, "Attribute Message"),

        /**
         * **Object Comment Message (0x000D)**:
         * Stores user-defined comments associated with an object.
         */
        ObjectCommentMessage((short) 13, "Object Comment Message"),

        /**
         * **Object Modification Time (Old) Message (0x000E)**:
         * Records the last modification time of an object (deprecated).
         */
        ObjectModificationTimeOldMessage((short) 14, "Object Modification Time (Old) Message"),

        /**
         * **Shared Message Table Message (0x000F)**:
         * Stores a table of shared header messages, reducing duplication.
         */
        SharedMessageTableMessage((short) 15, "Shared Message Table Message"),

        /**
         * **Object Header Continuation Message (0x0010)**:
         * Links to additional object header blocks when space runs out.
         */
        ObjectHeaderContinuationMessage((short) 16, "Object Header Continuation Message"),

        /**
         * **Symbol Table Message (0x0011)**:
         * Stores information about a group's symbol table for old-style groups.
         */
        SymbolTableMessage((short) 17, "Symbol Table Message"),

        /**
         * **Object Modification Time Message (0x0012)**:
         * Stores the last modification time of an object.
         */
        ObjectModificationTimeMessage((short) 18, "Object Modification Time Message"),

        /**
         * **B-tree ‘K’ Values Message (0x0013)**:
         * Specifies the B-tree splitting ratios for group structures.
         */
        BtreeKValuesMessage((short) 19, "B-tree ‘K’ Values Message"),

        /**
         * **Driver Info Message (0x0014)**:
         * Contains driver-specific metadata, usually for file drivers.
         */
        DriverInfoMessage((short) 20, "Driver Info Message"),

        /**
         * **Attribute Info Message (0x0015)**:
         * Stores metadata about attributes, including storage and indexing details.
         */
        AttributeInfoMessage((short) 21, "Attribute Info Message"),

        /**
         * **Object Reference Count Message (0x0016)**:
         * Stores the reference count of an object, preventing accidental deletion.
         */
        ObjectReferenceCountMessage((short) 22, "Object Reference Count Message"),

        /**
         * **File Space Info Message (0x0017)**:
         * Describes free space information for a file.
         */
        FileSpaceInfoMessage((short) 23, "File Space Info Message");

        private final short value;
        private final String name;

        MessageType(short value, String name) {
            this.value = value;
            this.name = name;
        }

        /**
         * Returns the MessageType associated with the given short value.
         *
         * @param value the short value of the message type
         * @return the corresponding MessageType
         * @throws IllegalArgumentException if the value does not match any known type
         */
        public static MessageType fromValue(short value) {
            for (MessageType type : MessageType.values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown MessageType value: " + value);
        }
    }
}
