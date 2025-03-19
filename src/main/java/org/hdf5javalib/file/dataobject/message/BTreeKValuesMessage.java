package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Represents a B-Tree K Values Message in the HDF5 file format.
 *
 * <p>The B-Tree K Values Message defines the split ratios (K values) for
 * different types of B-Trees used in an HDF5 file. These values determine
 * the number of keys per node in v1 B-Trees, which are used for indexing
 * chunked dataset storage and group structures.</p>
 *
 * <h2>Structure</h2>
 * <p>The B-Tree K Values Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the message format.</li>
 *   <li><b>Group Internal Node K (2 bytes)</b>: Specifies the number of
 *       keys in internal nodes of a group B-Tree.</li>
 *   <li><b>Group Leaf Node K (2 bytes)</b>: Specifies the number of keys
 *       in leaf nodes of a group B-Tree.</li>
 *   <li><b>Chunked Dataset Internal Node K (2 bytes)</b>: Specifies the
 *       number of keys in internal nodes of a chunked dataset B-Tree.</li>
 * </ul>
 *
 * <h2>B-Tree Usage</h2>
 * <p>HDF5 uses B-Trees (Version 1) to manage the following structures:</p>
 * <ul>
 *   <li><b>Group B-Trees:</b> Used for indexing objects within a group.</li>
 *   <li><b>Chunked Dataset B-Trees:</b> Used for indexing chunks in
 *       a dataset with chunked storage.</li>
 * </ul>
 *
 * <p>The K values defined in this message determine the efficiency of
 * indexing structures and affect the performance of searching and
 * modifying datasets.</p>
 *
 * <p>This class provides methods to parse and interpret B-Tree K Values
 * Messages based on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___b_t_r_e_e.html">
 *      HDF5 B-Tree Documentation</a>
 */
@Getter
public class BTreeKValuesMessage extends HdfMessage {
    private final int version;
    private final int indexedStorageInternalNodeK;
    private final int groupInternalNodeK;
    private final int groupLeafNodeK;

    public BTreeKValuesMessage(int version, int indexedStorageInternalNodeK, int groupInternalNodeK, int groupLeafNodeK) {
        super(MessageType.BtreeKValuesMessage, ()-> (short) (1+2+2+2), (byte)0);
        this.version = version;
        this.indexedStorageInternalNodeK = indexedStorageInternalNodeK;
        this.groupInternalNodeK = groupInternalNodeK;
        this.groupLeafNodeK = groupLeafNodeK;
    }

    public static BTreeKValuesMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse the message fields from the buffer
        int version = Byte.toUnsignedInt(buffer.get());
        if (version != 0) {
            throw new IllegalArgumentException("Unsupported B-tree K Values Message version: " + version);
        }
        int indexedStorageInternalNodeK = Short.toUnsignedInt(buffer.getShort());
        int groupInternalNodeK = Short.toUnsignedInt(buffer.getShort());
        int groupLeafNodeK = Short.toUnsignedInt(buffer.getShort());
        return new BTreeKValuesMessage(version, indexedStorageInternalNodeK, groupInternalNodeK, groupLeafNodeK);
    }

    @Override
    public String toString() {
        return "BTreeKValuesMessage{" +
                "version=" + version +
                ", indexedStorageInternalNodeK=" + indexedStorageInternalNodeK +
                ", groupInternalNodeK=" + groupInternalNodeK +
                ", groupLeafNodeK=" + groupLeafNodeK +
                '}';
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }
}
