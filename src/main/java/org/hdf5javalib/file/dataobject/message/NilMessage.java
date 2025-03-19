package org.hdf5javalib.file.dataobject.message;

import java.nio.ByteBuffer;

/**
 * Represents a Nil Message in the HDF5 file format.
 *
 * <p>The Nil Message is a placeholder message within an HDF5 object header.
 * It has no content and serves as an empty or unused slot in the message table.
 * Nil Messages may be created when messages are deleted or when space is reserved
 * for future message additions.</p>
 *
 * <h2>Structure</h2>
 * <p>The Nil Message consists of the following components:</p>
 * <ul>
 *   <li><b>Message Type</b>: Identified by a message type value of 0 (Nil).</li>
 *   <li><b>Message Size</b>: Typically zero but may have a reserved size.</li>
 *   <li><b>Flags</b>: Standard object header message flags, which are generally unused.</li>
 *   <li><b>Payload</b>: Empty (no data stored).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>Nil Messages serve several functions within an HDF5 file:</p>
 * <ul>
 *   <li>Maintaining message alignment within an object header.</li>
 *   <li>Preserving space for future messages in extensible metadata structures.</li>
 *   <li>Indicating deleted or unused object header messages.</li>
 * </ul>
 *
 * <p>Since Nil Messages contain no meaningful data, they are usually skipped
 * when parsing an object's metadata.</p>
 *
 * <p>This class provides methods to recognize and process Nil Messages based on
 * the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___o_b_j_e_c_t___h_e_a_d_e_r.html">
 *      HDF5 Object Header Documentation</a>
 */
public class NilMessage extends HdfMessage {

    public NilMessage(int size, byte flags) {
        super(MessageType.NilMessage, ()-> (short) size, flags);
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize) {
        // No data to parse for null message
        return new NilMessage(data.length, flags);
    }

    @Override
    public String toString() {
        return "NilMessage{" + super.getSizeMessageData() + "}";
    }

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }
}
