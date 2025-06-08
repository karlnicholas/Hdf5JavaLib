package org.hdf5javalib.redo.hdffile.dataobjects.messages;

import org.hdf5javalib.redo.hdffile.HdfDataFile;

import java.nio.ByteBuffer;

/**
 * Represents a Nil Message in the HDF5 file format.
 * <p>
 * The {@code NilMessage} class is a placeholder message within an HDF5 object header.
 * It has no content and serves as an empty or unused slot in the message table.
 * Nil Messages may be created when messages are deleted or when space is reserved
 * for future message additions.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Message Type</b>: Identified by a message type value of 0 (Nil).</li>
 *   <li><b>Message Size</b>: Typically zero but may have a reserved size.</li>
 *   <li><b>Flags</b>: Standard object header message flags, generally unused.</li>
 *   <li><b>Payload</b>: Empty (no data stored).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Maintaining message alignment within an object header.</li>
 *   <li>Preserving space for future messages in extensible metadata structures.</li>
 *   <li>Indicating deleted or unused object header messages.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class NilMessage extends HdfMessage {

    /**
     * Constructs a NilMessage with the specified components.
     *
     * @param size            the size of the message data (typically zero)
     * @param flags           the message flags
     * @param sizeMessageData the size of the message data in bytes
     */
    public NilMessage(int size, int flags, int sizeMessageData) {
        super(MessageType.NilMessage, sizeMessageData, flags);
    }

    /**
     * Parses a NilMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new NilMessage instance
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        // No data to parse for null message
        return new NilMessage(data.length, flags, (short) data.length);
    }

    /**
     * Returns a string representation of this NilMessage.
     *
     * @return a string describing the message size
     */
    @Override
    public String toString() {
        return "NilMessage(" + (getSizeMessageData() + 8) + "){}";
    }

    /**
     * Writes the NilMessage data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }
}