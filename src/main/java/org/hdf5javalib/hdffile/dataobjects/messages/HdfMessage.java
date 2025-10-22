package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.hdfjava.HdfDataFile;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Represents a base class for HDF5 Object Header Messages.
 * <p>
 * Each object in an HDF5 file has an object header containing metadata messages that
 * describe the object. These messages include essential information about datasets,
 * groups, and other HDF5 structures. The {@code HdfMessage} class serves as a base
 * for all such message types, encapsulating common properties and functionality.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Message Type (2 bytes)</b>: Identifies the type of message stored.</li>
 *   <li><b>Size of Message Data (2 bytes)</b>: The number of bytes in the message
 *       data, including padding to align to an 8-byte boundary.</li>
 *   <li><b>Message Flags (1 byte)</b>: A bit field defining message properties:
 *       <ul>
 *         <li>Bit 0: Message data is constant (e.g., Datatype Messages).</li>
 *         <li>Bit 1: Message is shared and stored in another location.</li>
 *         <li>Bit 2: Message should not be shared.</li>
 *         <li>Bit 3: Fail if message type is unknown and file is writable.</li>
 *         <li>Bit 4: Set bit 5 if message type is unknown and file is modified.</li>
 *         <li>Bit 5: Object modified by software unaware of this message.</li>
 *         <li>Bit 6: Message is shareable.</li>
 *         <li>Bit 7: Always fail if message type is unknown (even for read-only).</li>
 *       </ul>
 *   </li>
 *   <li><b>Reserved (3 bytes)</b>: Unused space for alignment.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>This class provides a foundation for handling various HDF5 metadata messages,
 * enabling structured parsing and serialization.</p>
 *
 * <h2>Usage</h2>
 * <ul>
 *   <li>{@link DataspaceMessage} – Defines dataset dimensions.</li>
 *   <li>{@link DatatypeMessage} – Specifies the type of data stored.</li>
 *   <li>{@link FillValueMessage} – Defines default fill values.</li>
 *   <li>{@link DataLayoutMessage} – Describes the storage layout.</li>
 *   <li>{@link AttributeMessage} – Stores user-defined metadata.</li>
 * </ul>
 *
 * @see HdfDataFile
 */
public abstract class HdfMessage {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HdfMessage.class);
    private static final int HDF_MESSAGE_RESERVED_SIZE=3;
    public static final int HDF_MESSAGE_PREAMBLE_SIZE=8;
    /**
     * The type of the message.
     */
    private final MessageType messageType;
    /**
     * The size of the message data in bytes, including padding.
     */
    private final int sizeMessageData;
    /**
     * The message flags indicating properties like constancy or shareability.
     */
    private final int messageFlags;

    /**
     * Constructs an HdfMessage with the specified components.
     *
     * @param messageType     the type of the message
     * @param sizeMessageData the size of the message data in bytes
     * @param messageFlags    the message flags
     */
    protected HdfMessage(MessageType messageType, int sizeMessageData, int messageFlags) {
        this.messageType = messageType;
        this.sizeMessageData = sizeMessageData;
        this.messageFlags = messageFlags;
    }

    /**
     * Writes the common message header data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the header data to
     */
    protected void writeMessageData(ByteBuffer buffer) {
        buffer.putShort((short) messageType.getValue());
        buffer.putShort((short) sizeMessageData);
        buffer.put((byte) messageFlags);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
        buffer.put((byte) 0);
    }

    /**
     * Writes the entire message to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message to
     */
    public abstract void writeMessageToByteBuffer(ByteBuffer buffer);

    public static class OBJECT_HEADER_PREFIX {
        final MessageType type;
        final int size;
        final int flags;
        final int order;

        OBJECT_HEADER_PREFIX(MessageType type, int size, int flags, int order) {
            this.type = type;
            this.size = size;
            this.flags = flags;
            this.order = order;
        }
    }

    public static Function<ByteBuffer, OBJECT_HEADER_PREFIX> V1_OBJECT_HEADER_READ_PREFIX = buffer-> {
        // Header Message Type (2 bytes, little-endian)
        MessageType type = MessageType.fromValue(buffer.getShort());
        int size = Short.toUnsignedInt(buffer.getShort());
        int flags = Byte.toUnsignedInt(buffer.get());
        buffer.position(buffer.position() + HDF_MESSAGE_RESERVED_SIZE); // Skip 3 reserved bytes
        return new OBJECT_HEADER_PREFIX(type, size, flags, 0);
    };

    public static Function<ByteBuffer, OBJECT_HEADER_PREFIX> V2_OBJECT_HEADER_READ_PREFIX = buffer-> {
        // Header Message Type (2 bytes, little-endian)
        MessageType type = MessageType.fromValue(buffer.get());
        int size = Short.toUnsignedInt(buffer.getShort());
        int flags = Byte.toUnsignedInt(buffer.get());
        return new OBJECT_HEADER_PREFIX(type, size, flags, 0);
    };

    public static Function<ByteBuffer, OBJECT_HEADER_PREFIX> V2OBJECT_HEADER_READ_PREFIX_WITHORDER = buffer-> {
        // Header Message Type (2 bytes, little-endian)
        MessageType type = MessageType.fromValue(buffer.get());
        int size = Short.toUnsignedInt(buffer.getShort());
        int flags = Byte.toUnsignedInt(buffer.get());
        int order = Short.toUnsignedInt(buffer.getShort());
        return new OBJECT_HEADER_PREFIX(type, size, flags, order);
    };

//    public static Function<ByteBuffer, OBJECT_HEADER_PREFIX> V2_OBJECT_HEADER_READ_PREFIX_V2 = buffer-> {
//        // Header Message Type (2 bytes, little-endian)
//        MessageType type = MessageType.fromValue(buffer.get());
//        int size = Short.toUnsignedInt(buffer.getShort());
//        int flags = Byte.toUnsignedInt(buffer.get());
//        int order = 0;
//        if ( (flags & (1 << 2)) != 0 ) {
//            order = Short.toUnsignedInt(buffer.getShort());
//        }
//        return new OBJECT_HEADER_PREFIX(type, size, flags, order);
//    };

    /**
     * Reads and parses a list of HdfMessages from the provided file channel.
     *
     * @param fileChannel      the SeekableByteChannel to read from
     * @param objectHeaderSize the size of the object header in bytes
     * @param hdfDataFile      the HDF5 file context for additional resources
     * @return a list of parsed HdfMessage instances
     * @throws IOException if an I/O error occurs
     */
    public static List<HdfMessage> readMessagesFromByteBuffer(
            SeekableByteChannel fileChannel,
            long objectHeaderSize,
            HdfDataFile hdfDataFile,
            Function<ByteBuffer, OBJECT_HEADER_PREFIX> prefixFunction
    ) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ByteBuffer buffer = ByteBuffer.allocate((int) objectHeaderSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();
        List<HdfMessage> messages = new ArrayList<>();

        byte[] OCHKSignature = new  byte[4];
        buffer.get(OCHKSignature);
        if (Arrays.compare(OCHKSignature, "OCHK".getBytes()) != 0 ) {
            buffer.rewind();
        }

        boolean readContinue = true;
        while (buffer.hasRemaining() && readContinue) {
            OBJECT_HEADER_PREFIX prefix = prefixFunction.apply(buffer);
            // Header Message Data
            byte[] messageData = new byte[prefix.size];
            buffer.get(messageData);


            HdfMessage hdfMessage = parseHeaderMessage(prefix.type, prefix.flags, messageData, hdfDataFile);
            log.trace("Read: hdfMessage.sizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE = {} {}", hdfMessage.messageType, hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE);
            // Add the message to the list
            messages.add(hdfMessage);
            //TODO: no good.
            if ( buffer.hasRemaining() ) {
                if ( buffer.remaining() <= 4 ) {
                    readContinue = false;
                } else {
                    buffer.mark();
                    MessageType type = MessageType.fromValue(buffer.get());
                    buffer.reset();
                    if ( buffer.remaining() < 10 ) {
                        readContinue = false;
                    }
                }
            }
        }
        return messages;
    }

    /**
     * Creates an instance of the appropriate HdfMessage subclass based on the message type.
     *
     * @param type        the message type
     * @param flags       the message flags
     * @param data        the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new HdfMessage instance
     * @throws IllegalArgumentException if the message type is unknown
     */
    protected static HdfMessage parseHeaderMessage(MessageType type, int flags, byte[] data, HdfDataFile hdfDataFile) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        log.trace("type:flags:length {} {} {}", type, flags, data.length);
        return switch (type) {
            case NIL_MESSAGE -> NilMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case DATASPACE_MESSAGE -> DataspaceMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case LINK_INFO_MESSAGE ->  LinkInfoMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case DATATYPE_MESSAGE -> DatatypeMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case FILL_MESSAGE -> FillMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case FILL_VALUE_MESSAGE -> FillValueMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case LINK_MESSAGE -> LinkMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case EXTERNAL_DATA_FILES_MESSAGE -> ExternalDataFilesMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case DATA_LAYOUT_MESSAGE -> DataLayoutMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case GROUP_INFO_MESSAGE ->  GroupInfoMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case FILTER_PIPELINE_MESSAGE ->  FilterPipelineMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case ATTRIBUTE_MESSAGE -> AttributeMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case OBJECT_HEADER_CONTINUATION_MESSAGE -> ObjectHeaderContinuationMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case SYMBOL_TABLE_MESSAGE -> SymbolTableMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case OBJECT_MODIFICATION_TIME_MESSAGE -> ObjectModificationTimeMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case BTREE_K_VALUES_MESSAGE -> BTreeKValuesMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case ATTRIBUTE_INFO_MESSAGE -> AttributeInfoMessage.parseHeaderMessage(flags, data, hdfDataFile);
            case OBJECT_REFERENCE_COUNT_MESSAGE ->  ObjectReferenceCountMessage.parseHeaderMessage(flags, data, hdfDataFile);
            default -> throw new IllegalArgumentException("Unknown message type: " + type);
        };
    }

    /**
     * Parses a continuation message block to retrieve additional HdfMessages.
     *
     * @param fileChannel                     the SeekableByteChannel to read from
     * @param objectHeaderContinuationMessage the continuation message defining the block
     * @param hdfDataFile                     the HDF5 file context for additional resources
     * @return a list of parsed HdfMessage instances from the continuation block
     * @throws IOException if an I/O error occurs
     */
    public static List<HdfMessage> parseContinuationMessage(
            SeekableByteChannel fileChannel,
            ObjectHeaderContinuationMessage objectHeaderContinuationMessage,
            HdfDataFile hdfDataFile,
            Function<ByteBuffer, OBJECT_HEADER_PREFIX> prefixFunction
    ) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        long continuationOffset = objectHeaderContinuationMessage.getContinuationOffset().getInstance(Long.class);
        long continuationSize = objectHeaderContinuationMessage.getContinuationSize().getInstance(Long.class);

        // Move to the continuation block offset
        fileChannel.position(continuationOffset);

        // Parse the continuation block messages
        return new ArrayList<>(readMessagesFromByteBuffer(fileChannel, continuationSize, hdfDataFile, prefixFunction));
    }

    public int getSizeMessageData() {
        return sizeMessageData;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public int getMessageFlags() {
        return messageFlags;
    }

    /**
     * Enum representing various HDF5 message types.
     */
    public enum MessageType {
        /**
         * NIL Message (0x0000): A placeholder message to be ignored.
         */
        NIL_MESSAGE((short) 0, "NIL Message"),

        /**
         * Dataspace Message (0x0001): Defines dataset dimensions and size.
         */
        DATASPACE_MESSAGE((short) 1, "Dataspace Message"),

        /**
         * Link Info Message (0x0002): Stores metadata about links.
         */
        LINK_INFO_MESSAGE((short) 2, "Link Info Message"),

        /**
         * Datatype Message (0x0003): Specifies the datatype for dataset elements.
         */
        DATATYPE_MESSAGE((short) 3, "Datatype Message"),

        /**
         * Data Storage - Fill Value (Old) Message (0x0004): Defines fill values (deprecated).
         */
        FILL_MESSAGE((short) 4, "Data Storage - Fill Value (Old) Message"),

        /**
         * Data Storage - Fill Value Message (0x0005): Defines fill values.
         */
        FILL_VALUE_MESSAGE((short) 5, "Data Storage - Fill Value Message"),

        /**
         * Link Message (0x0006): Represents a link to another object.
         */
        LINK_MESSAGE((short) 6, "Link Message"),

        /**
         * Data Storage - External Data Files Message (0x0007): Indicates external file storage.
         */
        EXTERNAL_DATA_FILES_MESSAGE((short) 7, "Data Storage - External Data Files Message"),

        /**
         * Data Layout Message (0x0008): Describes data storage layout.
         */
        DATA_LAYOUT_MESSAGE((short) 8, "Data Layout Message"),

        /**
         * Bogus Message (0x0009): Used for internal testing.
         */
        BOGUS_MESSAGE((short) 9, "Bogus Message"),

        /**
         * Group Info Message (0x000A): Provides group metadata.
         */
        GROUP_INFO_MESSAGE((short) 10, "Group Info Message"),

        /**
         * Data Storage - Filter Pipeline Message (0x000B): Defines data filters.
         */
        FILTER_PIPELINE_MESSAGE((short) 11, "Data Storage - Filter Pipeline Message"),

        /**
         * Attribute Message (0x000C): Stores user-defined metadata attributes.
         */
        ATTRIBUTE_MESSAGE((short) 12, "Attribute Message"),

        /**
         * Object Comment Message (0x000D): Stores user-defined comments.
         */
        OBJECT_COMMENT_MESSAGE((short) 13, "Object Comment Message"),

        /**
         * Object Modification Time (Old) Message (0x000E): Records modification time (deprecated).
         */
        OBJECT_MODIFICATION_TIME_OLD_MESSAGE((short) 14, "Object Modification Time (Old) Message"),

        /**
         * Shared Message Table Message (0x000F): Stores shared header messages.
         */
        SHARED_MESSAGE_TABLE_MESSAGE((short) 15, "Shared Message Table Message"),

        /**
         * Object Header Continuation Message (0x0010): Links to additional header blocks.
         */
        OBJECT_HEADER_CONTINUATION_MESSAGE((short) 16, "Object Header Continuation Message"),

        /**
         * Symbol Table Message (0x0011): Stores group symbol table information.
         */
        SYMBOL_TABLE_MESSAGE((short) 17, "Symbol Table Message"),

        /**
         * Object Modification Time Message (0x0012): Records modification time.
         */
        OBJECT_MODIFICATION_TIME_MESSAGE((short) 18, "Object Modification Time Message"),

        /**
         * B-tree ‘K’ Values Message (0x0013): Specifies B-tree splitting ratios.
         */
        BTREE_K_VALUES_MESSAGE((short) 19, "B-tree ‘K’ Values Message"),

        /**
         * Driver Info Message (0x0014): Contains driver-specific metadata.
         */
        DRIVER_INFO_MESSAGE((short) 20, "Driver Info Message"),

        /**
         * Attribute Info Message (0x0015): Stores attribute metadata.
         */
        ATTRIBUTE_INFO_MESSAGE((short) 21, "Attribute Info Message"),

        /**
         * Object Reference Count Message (0x0016): Stores object reference count.
         */
        OBJECT_REFERENCE_COUNT_MESSAGE((short) 22, "Object Reference Count Message"),

        /**
         * File Space Info Message (0x0017): Describes file free space information.
         */
        FILE_SPACE_INFO_MESSAGE((short) 23, "File Space Info Message");

        private final int value;
        private final String name;

        MessageType(short value, String name) {
            this.value = value;
            this.name = name;
        }

        /**
         * Retrieves the MessageType corresponding to the given value.
         *
         * @param value the numeric value of the message type
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

        /**
         * Returns the numeric value of the message type.
         *
         * @return short of value
         */
        public int getValue() {
            return value;
        }
    }
}