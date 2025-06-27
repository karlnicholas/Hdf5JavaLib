package org.hdf5javalib.maydo.hdffile.dataobjects.messages;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hdf5javalib.maydo.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents an Object Header Continuation Message in the HDF5 file format.
 * <p>
 * The {@code ObjectHeaderContinuationMessage} class is used when an object's header
 * contains more metadata than can fit within a single object header block. This
 * message points to a continuation block elsewhere in the file that stores additional
 * object header messages.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Address (8 bytes, file offset)</b>: The location in the file where the
 *       continuation block starts.</li>
 *   <li><b>Length (4 bytes)</b>: The size of the continuation block in bytes.</li>
 *   <li><b>Continuation Block</b>: Additional object header messages, formatted
 *       similarly to the original object header.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Handling large object headers that exceed allocated space.</li>
 *   <li>Supporting extensible metadata for complex datasets and groups.</li>
 *   <li>Maintaining efficient storage and retrieval of object metadata.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class ObjectHeaderContinuationMessage extends HdfMessage {
    /**
     * The offset of the continuation block in the file.
     */
    private HdfFixedPoint continuationOffset;
    /**
     * The size of the continuation block in bytes.
     */
    private HdfFixedPoint continuationSize;

    /**
     * Constructs an ObjectHeaderContinuationMessage with the specified components.
     *
     * @param continuationOffset the offset of the continuation block
     * @param continuationSize   the size of the continuation block
     * @param flags              message flags
     * @param sizeMessageData    the size of the message data in bytes
     */
    public ObjectHeaderContinuationMessage(final HdfFixedPoint continuationOffset, final HdfFixedPoint continuationSize, int flags, int sizeMessageData) {
        super(MessageType.ObjectHeaderContinuationMessage, sizeMessageData, flags);
        this.continuationOffset = continuationOffset;
        this.continuationSize = continuationSize;
    }

    /**
     * Parses an ObjectHeaderContinuationMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for datatype resources
     * @return a new ObjectHeaderContinuationMessage instance
     */
    public static ObjectHeaderContinuationMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse the continuation offset and size
        HdfFixedPoint continuationOffset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint continuationSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), buffer);
        return new ObjectHeaderContinuationMessage(continuationOffset, continuationSize, flags, data.length);
    }

    /**
     * Returns a string representation of this ObjectHeaderContinuationMessage.
     *
     * @return a string describing the message size, continuation offset, and size
     */
    @Override
    public String toString() {
        return "ObjectHeaderContinuationMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "continuationOffset=" + continuationOffset +
                ", continuationSize=" + continuationSize +
                '}';
    }

    /**
     * Writes the ObjectHeaderContinuationMessage data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write continuation offset
        writeFixedPointToBuffer(buffer, continuationOffset);
        // Write continuation size
        writeFixedPointToBuffer(buffer, continuationSize);
    }

    public HdfFixedPoint getContinuationOffset() {
        return continuationOffset;
    }

    public HdfFixedPoint getContinuationSize() {
        return continuationSize;
    }

    public void setContinuationOffset(HdfFixedPoint continuationOffset) {
        this.continuationOffset = continuationOffset;
    }

    public void setContinuationSize(HdfFixedPoint continuationSize) {
        this.continuationSize = continuationSize;
    }
}