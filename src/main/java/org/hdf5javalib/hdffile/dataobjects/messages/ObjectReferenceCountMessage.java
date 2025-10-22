package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.hdfjava.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class ObjectReferenceCountMessage extends HdfMessage {
    /**
     * The version of the fill value message format.
     */
    private final int version;
    private final long referenceCount;

    /**
     * Public constructor
     * @param version Message version
     * @param referenceCount referenceCount in Message
     * @param sizeMessageData size of message data
     * @param messageFlags messageflags
     */
    public ObjectReferenceCountMessage(int version, long referenceCount, int sizeMessageData, int messageFlags) {
        super(MessageType.OBJECT_REFERENCE_COUNT_MESSAGE, sizeMessageData, messageFlags);
        this.version = version;
        this.referenceCount = referenceCount;
    }

    /**
     * Parses a FillValueMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new FillValueMessage instance parsed from the data
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse the first 4 bytes
        int version = Byte.toUnsignedInt(buffer.get());
        long referenceCount = Integer.toUnsignedLong(buffer.getInt());
        return new ObjectReferenceCountMessage(version, referenceCount, flags, data.length);
    }

    /**
     * Returns a string representation of this FillValueMessage.
     *
     * @return a string describing the message size, version, allocation times, and fill value details
     */
    @Override
    public String toString() {
        return "ObjectReferenceCountMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", referenceCount=" + referenceCount +
                '}';
    }


    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        // not implemented
    }
}