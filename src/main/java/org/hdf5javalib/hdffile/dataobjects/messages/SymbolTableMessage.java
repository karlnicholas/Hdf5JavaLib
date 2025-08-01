package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Symbol Table Message in the HDF5 file format.
 * <p>
 * The {@code SymbolTableMessage} class provides a reference to a symbol table, which is
 * used to store entries for objects (such as datasets and groups) within an HDF5 group.
 * It is an essential component for managing hierarchical relationships in HDF5 files.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>B-Tree Address (8 bytes)</b>: The file offset of the B-Tree indexing the group’s entries.</li>
 *   <li><b>Local Heap Address (8 bytes)</b>: The file offset of the local heap storing names of the group’s entries.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Storing and organizing object metadata within groups.</li>
 *   <li>Providing efficient indexing via a B-Tree for quick object lookup.</li>
 *   <li>Managing object names via a local heap structure.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class SymbolTableMessage extends HdfMessage {
    /**
     * The file offset of the B-Tree indexing the group’s entries.
     */
    private final HdfFixedPoint bTreeAddress;
    /**
     * The file offset of the local heap storing names of the group’s entries.
     */
    private final HdfFixedPoint localHeapAddress;

    /**
     * Constructs a SymbolTableMessage with the specified components.
     *
     * @param bTreeAddress     the file offset of the B-Tree
     * @param localHeapAddress the file offset of the local heap
     * @param flags            message flags
     * @param sizeMessageData  the size of the message data in bytes
     */
    public SymbolTableMessage(HdfFixedPoint bTreeAddress, HdfFixedPoint localHeapAddress, int flags, int sizeMessageData) {
        super(MessageType.SYMBOL_TABLE_MESSAGE, sizeMessageData, flags);
        this.bTreeAddress = bTreeAddress;
        this.localHeapAddress = localHeapAddress;
    }

    /**
     * Parses a SymbolTableMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for datatype resources
     * @return a new SymbolTableMessage instance
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        HdfFixedPoint bTreeAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint localHeapAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        return new SymbolTableMessage(bTreeAddress, localHeapAddress, flags, data.length);
    }

    /**
     * Writes the SymbolTableMessage data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Write B-tree address
        writeFixedPointToBuffer(buffer, bTreeAddress);
        // Write Local Heap address
        writeFixedPointToBuffer(buffer, localHeapAddress);
    }

    /**
     * Returns a string representation of this SymbolTableMessage.
     *
     * @return a string describing the message size, B-Tree address, and local heap address
     */
    @Override
    public String toString() {
        try {
            return "SymbolTableMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                    "bTreeAddress=" + bTreeAddress.getInstance(Long.class) +
                    ", localHeapAddress=" + localHeapAddress.getInstance(Long.class) +
                    '}';
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException | IOException e) {
            throw new IllegalStateException(e);
        }
    }
}