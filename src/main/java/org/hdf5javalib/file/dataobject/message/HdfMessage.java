package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

@Getter
public abstract class HdfMessage {
    private final MessageType messageType;
    @Setter
    private short sizeMessageData;
    private final byte messageFlags;

    protected HdfMessage(MessageType messageType, Supplier<Short> sizeSupplier, byte messageFlags) {
        this.messageType = messageType;
        this.sizeMessageData = sizeSupplier.get();
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

    public abstract void writeToByteBuffer(ByteBuffer buffer);

    /**
     * Enum representing various HDF5 message types.
     */
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

        public short getValue() {
            return value;
        }

        public String getName() {
            return name;
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
