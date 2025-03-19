package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Symbol Table Message in the HDF5 file format.
 *
 * <p>The Symbol Table Message provides a reference to a symbol table, which is
 * used to store entries for objects (such as datasets and groups) within an
 * HDF5 group. It is an essential component of the HDF5 file structure for
 * managing hierarchical relationships.</p>
 *
 * <h2>Structure</h2>
 * <p>The Symbol Table Message consists of the following components:</p>
 * <ul>
 *   <li><b>Version (1 byte)</b>: Identifies the version of the symbol table message format.</li>
 *   <li><b>BTrees Address (8 bytes)</b>: Specifies the location in the file of the
 *       B-Tree that indexes the group’s entries.</li>
 *   <li><b>Heap Address (8 bytes)</b>: Specifies the location of the local heap
 *       that stores names of the group’s entries.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>The Symbol Table Message is used for:</p>
 * <ul>
 *   <li>Storing and organizing object metadata within groups.</li>
 *   <li>Providing efficient indexing via a B-Tree for quick object lookup.</li>
 *   <li>Managing long object names via a heap structure.</li>
 * </ul>
 *
 * <h2>Processing</h2>
 * <p>When an HDF5 group contains multiple objects, the Symbol Table Message
 * directs the reader to the B-Tree and heap where the names and metadata of
 * the group’s members are stored. This allows efficient retrieval and
 * organization of objects within the group.</p>
 *
 * <p>This class provides methods to parse and interpret Symbol Table Messages
 * based on the HDF5 file specification.</p>
 *
 * @see <a href="https://docs.hdfgroup.org/hdf5/develop/group___s_y_m_b_o_l_t_a_b_l_e.html">
 *      HDF5 Symbol Table Documentation</a>
 */
@Getter
public class SymbolTableMessage extends HdfMessage {
    private final HdfFixedPoint bTreeAddress;
    private final HdfFixedPoint localHeapAddress;

    // Constructor to create SymbolTableMessage directly with values
    public SymbolTableMessage(HdfFixedPoint bTreeAddress, HdfFixedPoint localHeapAddress) {
        super(MessageType.SymbolTableMessage, ()-> (short) (bTreeAddress.getDatatype().getSize() + localHeapAddress.getDatatype().getSize()), (byte)0);
        this.bTreeAddress = bTreeAddress;
        this.localHeapAddress = localHeapAddress;
    }

    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        HdfFixedPoint bTreeAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
        HdfFixedPoint localHeapAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
        return new SymbolTableMessage(bTreeAddress, localHeapAddress);
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write B-tree address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, bTreeAddress);

        // Write Local Heap address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, localHeapAddress);
    }

    @Override
    public String toString() {
        return "SymbolTableMessage{" +
                "bTreeAddress=" + bTreeAddress.getInstance(Long.class) +
                ", localHeapAddress=" + localHeapAddress.getInstance(Long.class) +
                '}';
    }
}
