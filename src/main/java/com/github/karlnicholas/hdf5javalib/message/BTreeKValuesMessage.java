package com.github.karlnicholas.hdf5javalib.message;

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
public class BTreeKValuesMessage implements HdfMessage {
    private int version;
    private int indexedStorageInternalNodeK;
    private int groupInternalNodeK;
    private int groupLeafNodeK;

    public BTreeKValuesMessage(int version, int indexedStorageInternalNodeK, int groupInternalNodeK, int groupLeafNodeK) {
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

    public int getVersion() {
        return version;
    }

    public int getIndexedStorageInternalNodeK() {
        return indexedStorageInternalNodeK;
    }

    public int getGroupInternalNodeK() {
        return groupInternalNodeK;
    }

    public int getGroupLeafNodeK() {
        return groupLeafNodeK;
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
    public void writeToByteBuffer(ByteBuffer buffer, int offsetSize) {

    }
}
