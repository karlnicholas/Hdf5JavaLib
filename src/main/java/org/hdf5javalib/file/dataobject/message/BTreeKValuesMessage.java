package org.hdf5javalib.file.dataobject.message;

import org.hdf5javalib.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Represents a B-Tree K Values Message in the HDF5 file format.
 * <p>
 * The {@code BTreeKValuesMessage} class defines the split ratios (K values) for
 * different types of B-Trees used in an HDF5 file. These values determine the
 * number of keys per node in v1 B-Trees, which are used for indexing chunked
 * dataset storage and group structures.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the message format.</li>
 *   <li><b>Group Internal Node K (2 bytes)</b>: The number of keys in internal
 *       nodes of a group B-Tree.</li>
 *   <li><b>Group Leaf Node K (2 bytes)</b>: The number of keys in leaf nodes
 *       of a group B-Tree.</li>
 *   <li><b>Chunked Dataset Internal Node K (2 bytes)</b>: The number of keys
 *       in internal nodes of a chunked dataset B-Tree.</li>
 * </ul>
 *
 * <h2>B-Tree Usage</h2>
 * <ul>
 *   <li><b>Group B-Trees:</b> Used for indexing objects within a group.</li>
 *   <li><b>Chunked Dataset B-Trees:</b> Used for indexing chunks in a dataset
 *       with chunked storage.</li>
 * </ul>
 *
 * @see org.hdf5javalib.file.dataobject.message.HdfMessage
 * @see org.hdf5javalib.HdfDataFile
 */
public class BTreeKValuesMessage extends HdfMessage {
    /** The version of the message format. */
    private final int version;
    /** The number of keys in internal nodes of a chunked dataset B-Tree. */
    private final int indexedStorageInternalNodeK;
    /** The number of keys in internal nodes of a group B-Tree. */
    private final int groupInternalNodeK;
    /** The number of keys in leaf nodes of a group B-Tree. */
    private final int groupLeafNodeK;

    /**
     * Constructs a BTreeKValuesMessage with the specified components.
     *
     * @param version                    the version of the message format
     * @param indexedStorageInternalNodeK the number of keys in internal nodes of a chunked dataset B-Tree
     * @param groupInternalNodeK         the number of keys in internal nodes of a group B-Tree
     * @param groupLeafNodeK             the number of keys in leaf nodes of a group B-Tree
     * @param flags                      message flags
     * @param sizeMessageData            the size of the message data in bytes
     */
    public BTreeKValuesMessage(int version, int indexedStorageInternalNodeK, int groupInternalNodeK, int groupLeafNodeK, byte flags, short sizeMessageData) {
        super(MessageType.BtreeKValuesMessage, sizeMessageData, flags);
        this.version = version;
        this.indexedStorageInternalNodeK = indexedStorageInternalNodeK;
        this.groupInternalNodeK = groupInternalNodeK;
        this.groupLeafNodeK = groupLeafNodeK;
    }

    /**
     * Parses a BTreeKValuesMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new BTreeKValuesMessage instance parsed from the data
     * @throws IllegalArgumentException if the message version is unsupported
     */
    public static BTreeKValuesMessage parseHeaderMessage(byte flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse the message fields from the buffer
        int version = Byte.toUnsignedInt(buffer.get());
        if (version != 0) {
            throw new IllegalArgumentException("Unsupported B-tree K Values Message version: " + version);
        }
        int indexedStorageInternalNodeK = Short.toUnsignedInt(buffer.getShort());
        int groupInternalNodeK = Short.toUnsignedInt(buffer.getShort());
        int groupLeafNodeK = Short.toUnsignedInt(buffer.getShort());
        return new BTreeKValuesMessage(version, indexedStorageInternalNodeK, groupInternalNodeK, groupLeafNodeK, flags, (short) data.length);
    }

    /**
     * Returns a string representation of this BTreeKValuesMessage.
     *
     * @return a string describing the message size, version, and K values
     */
    @Override
    public String toString() {
        return "BTreeKValuesMessage("+(getSizeMessageData()+8)+"){" +
                "version=" + version +
                ", indexedStorageInternalNodeK=" + indexedStorageInternalNodeK +
                ", groupInternalNodeK=" + groupInternalNodeK +
                ", groupLeafNodeK=" + groupLeafNodeK +
                '}';
    }

    /**
     * Writes the B-Tree K Values message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }
}