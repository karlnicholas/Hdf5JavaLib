package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import lombok.Setter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;


/**
 * Represents an Object Header Continuation Message in the HDF5 file format.
 *
 * <p>The Object Header Continuation Message is used when an object's header
 * contains more metadata than can fit within a single object header block.
 * This message points to a continuation block elsewhere in the file that
 * stores additional object header messages.</p>
 *
 * <h2>Structure</h2>
 * <p>The Object Header Continuation Message consists of the following components:</p>
 * <ul>
 *   <li><b>Address (8 bytes, file offset)</b>: Specifies the location in the file
 *       where the continuation block starts.</li>
 *   <li><b>Length (4 bytes)</b>: Defines the size of the continuation block in bytes.</li>
 *   <li><b>Continuation Block</b>: Contains additional object header messages,
 *       formatted similarly to the original object header.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>Object Header Continuation Messages are essential for:</p>
 * <ul>
 *   <li>Handling large object headers that exceed the allocated space.</li>
 *   <li>Supporting extensible metadata structures for complex datasets and groups.</li>
 *   <li>Maintaining efficient storage and retrieval of object metadata in HDF5 files.</li>
 * </ul>
 *
 * <h2>Processing</h2>
 * <p>When parsing an HDF5 file, encountering an Object Header Continuation Message
 * requires reading the referenced continuation block and processing its contained
 * object header messages.</p>
 *
 * <p>This class provides methods to parse and interpret Object Header Continuation Messages
 * based on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___o_b_j_e_c_t___h_e_a_d_e_r.html">
 *      HDF5 Object Header Documentation</a>
 */
@Getter
@Setter
public class ObjectHeaderContinuationMessage extends HdfMessage {
    private HdfFixedPoint continuationOffset; // Offset of the continuation block
    private HdfFixedPoint continuationSize;   // Size of the continuation block

    public ObjectHeaderContinuationMessage(final HdfFixedPoint continuationOffset, final HdfFixedPoint continuationSize, byte flags, short sizeMessageData) {
        super(MessageType.ObjectHeaderContinuationMessage, sizeMessageData, flags);
        this.continuationOffset = continuationOffset;
        this.continuationSize = continuationSize;
    }

    public static ObjectHeaderContinuationMessage parseHeaderMessage(byte flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        // Parse the continuation offset and size
        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint continuationOffset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint continuationSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFixedPointDatatypeForLength(), buffer);
        return new ObjectHeaderContinuationMessage(continuationOffset, continuationSize, flags, (short) data.length);
    }

    @Override
    public String toString() {
        return "ObjectHeaderContinuationMessage("+(getSizeMessageData()+8)+"){" +
                "continuationOffset=" + continuationOffset +
                ", continuationSize=" + continuationSize +
                '}';
    }

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write B-tree address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, continuationOffset);

        // Write Local Heap address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, continuationSize);
    }
}
