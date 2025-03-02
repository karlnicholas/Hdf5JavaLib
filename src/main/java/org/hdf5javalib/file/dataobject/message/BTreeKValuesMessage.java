package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Represents the B-tree 'K' Values message as per the HDF5 specification.
 * Header Message Name: B-tree ‘K’ Values
 * Header Message Type: 0x0013
 * Length: Fixed
 * Status: Optional; may not be repeated.
 * Description: Retrieves non-default ‘K’ values for internal and leaf nodes
 * of a group or indexed storage v1 B-trees. This message is only found in the superblock extension.
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
