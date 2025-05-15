package org.hdf5javalib.redo.hdffile.dataobjects;

import org.hdf5javalib.redo.AllocationRecord;
import org.hdf5javalib.redo.AllocationType;
import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.HdfFileAllocation;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.ObjectHeaderContinuationMessage;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.hdf5javalib.redo.HdfFileAllocation.SUPERBLOCK_OFFSET;
import static org.hdf5javalib.redo.HdfFileAllocation.SUPERBLOCK_SIZE;
import static org.hdf5javalib.redo.hdffile.dataobjects.messages.HdfMessage.parseContinuationMessage;
import static org.hdf5javalib.redo.hdffile.dataobjects.messages.HdfMessage.readMessagesFromByteBuffer;

/**
 * Represents the version 1 object header prefix for an HDF5 data object.
 * <p>
 * The {@code HdfObjectHeaderPrefixV1} class encapsulates the metadata for an HDF5 data object's
 * header, including version, reference count, header size, and a list of header messages. It provides
 * methods to read the header from a file channel, write it to a byte channel or buffer, and manage
 * continuation messages for large headers.
 * </p>
 */
public class HdfObjectHeaderPrefixV1 extends AllocationRecord {
    /** The version of the object header (1 byte). */
    private final int version;

    /** The reference count for the object (4 bytes). */
    private final long objectReferenceCount;

    /** The size of the object header (4 bytes). */
    private final long objectHeaderSize;

    /** The list of header messages associated with the object. */
    private final List<HdfMessage> headerMessages;

    /**
     * Constructs an HdfObjectHeaderPrefixV1 with application-defined values.
     *
     * @param version             the version of the object header
     * @param objectReferenceCount the reference count for the object
     * @param objectHeaderSize    the size of the object header
     * @param headerMessages      the list of header messages
     */
    public HdfObjectHeaderPrefixV1(int version, long objectReferenceCount, long objectHeaderSize, List<HdfMessage> headerMessages,
                                   HdfDataFile hdfDataFile, String name, HdfFixedPoint offset
    ) {
        super(AllocationType.DATASET_OBJECT_HEADER, name, offset,
                HdfWriteUtils.hdfFixedPointFromValue(HdfFileAllocation.DATA_OBJECT_HEADER_MESSAGE_SIZE, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength())
        );
        this.version = version;
        this.objectReferenceCount = objectReferenceCount;
        this.objectHeaderSize = objectHeaderSize;
        this.headerMessages = headerMessages;
    }

    /**
     * Reads an HdfObjectHeaderPrefixV1 from a file channel.
     * <p>
     * Parses the fixed-size header (version, reference count, header size) and header messages,
     * including any continuation messages, from the specified file channel.
     * </p>
     *
     * @param fileChannel the seekable byte channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfObjectHeaderPrefixV1 instance
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if reserved fields are non-zero
     */
    public static HdfObjectHeaderPrefixV1 readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile
    ) throws IOException {
        long offset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN); // Buffer for the fixed-size header
        fileChannel.read(buffer);
        buffer.flip();

        // Parse Version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());

        // Reserved (1 byte, should be zero)
        int reserved = Byte.toUnsignedInt(buffer.get());
        if (reserved != 0) {
            throw new IllegalArgumentException("Reserved byte in Data Object Header Prefix is not zero.");
        }

        // Total Number of Header Messages (2 bytes, little-endian)
        int totalHeaderMessages = Short.toUnsignedInt(buffer.getShort());

        // Object Reference Count (4 bytes, little-endian)
        long objectReferenceCount = Integer.toUnsignedLong(buffer.getInt());

        // Object Header Size (4 bytes, little-endian)
        short objectHeaderSize = (short) buffer.getInt();

        // Reserved (4 bytes, should be zero)
        int reservedInt = buffer.getInt();
        if (reservedInt != 0) {
            throw new IllegalArgumentException("Reserved integer in Data Object Header Prefix is not zero.");
        }
        List<HdfMessage> dataObjectHeaderMessages = new ArrayList<>(readMessagesFromByteBuffer(fileChannel, objectHeaderSize, hdfDataFile));
        for (HdfMessage hdfMessage : dataObjectHeaderMessages) {
            if (hdfMessage instanceof ObjectHeaderContinuationMessage) {
                dataObjectHeaderMessages.addAll(parseContinuationMessage(fileChannel, (ObjectHeaderContinuationMessage) hdfMessage, hdfDataFile));
                break;
            }
        }

        // Create the instance
        return new HdfObjectHeaderPrefixV1(version, objectReferenceCount, objectHeaderSize, dataObjectHeaderMessages,
                hdfDataFile, "Group Object Header", HdfWriteUtils.hdfFixedPointFromValue(offset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()));
    }

    /**
     * Writes the object header as a group header to a byte channel.
     * <p>
     * Serializes the header fields (version, reference count, header size) and all header
     * messages to the specified byte channel, ensuring proper alignment and handling of
     * continuation messages.
     * </p>
     *
     * @param seekableByteChannel the byte channel to write to
     * @param fileAllocation      the file allocation manager
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if the buffer overflows
     */
    public void writeAsGroupToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException {
        int currentSize = 16;
        int i = 0;
        while (i < headerMessages.size()) {
            HdfMessage hdfMessage = headerMessages.get(i);
            currentSize += hdfMessage.getSizeMessageData() + 8;
            currentSize = (currentSize + 7) & ~7;
            i++;
        }
        ByteBuffer buffer = ByteBuffer.allocate(currentSize).order(ByteOrder.LITTLE_ENDIAN);

        // Write version (1 byte)
        buffer.put((byte) version);

        // Write reserved byte (must be 0)
        buffer.put((byte) 0);

        // Write total header messages (2 bytes, little-endian)
        buffer.putShort((short) headerMessages.size());

        // Write object reference count (4 bytes, little-endian)
        buffer.putInt((int) objectReferenceCount);

        // Write object header size (4 bytes, little-endian)
        buffer.putInt((int) objectHeaderSize);

        // Write reserved field (must be 0) (4 bytes)
        buffer.putInt(0);

        // Write messages, handling continuation as first message but splitting at 6
        for (HdfMessage hdfMessage : headerMessages) {
            hdfMessage.writeMessageToByteBuffer(buffer);

            // Pad to 8-byte boundary
            int position = buffer.position();
            buffer.position((position + 7) & ~7);

            // assuming this doesn't happen.
            if (buffer.position() > buffer.capacity()) {
                throw new IllegalStateException("Buffer overflow writing group header messages to byte channel.");
            }
        }
        buffer.rewind();
        long rootGroupOffset = SUPERBLOCK_OFFSET + SUPERBLOCK_SIZE;

        seekableByteChannel.position(rootGroupOffset);
        while (buffer.hasRemaining()) {
            seekableByteChannel.write(buffer);
        }
    }

    /**
     * Writes the initial message block of the object header to a buffer.
     * <p>
     * Serializes the header fields (version, reference count, header size) and the initial
     * set of header messages to the specified buffer, padding to an 8-byte boundary.
     * </p>
     *
     * @param buffer the ByteBuffer to write to
     */
    public void writeInitialMessageBlockToBuffer(ByteBuffer buffer) {
        // Write version (1 byte)
        buffer.put((byte) version);

        // Write reserved byte (must be 0)
        buffer.put((byte) 0);

        // Write total header messages (2 bytes, little-endian)
        buffer.putShort((short) headerMessages.size());

        // Write object reference count (4 bytes, little-endian)
        buffer.putInt((int) objectReferenceCount);

        // Write object header size (4 bytes, little-endian)
        buffer.putInt((int) objectHeaderSize);

        // Write reserved field (must be 0) (4 bytes)
        buffer.putInt(0);

        // Write messages, handling continuation as first message but splitting at 6
        for (HdfMessage hdfMessage : headerMessages) {
            hdfMessage.writeMessageToByteBuffer(buffer);

            // Pad to 8-byte boundary
            int position = buffer.position();
            buffer.position((position + 7) & ~7);

            if (buffer.position() >= buffer.capacity()) {
                break;
            }
        }
    }

    /**
     * Writes the continuation message block of the object header to a buffer.
     * <p>
     * Serializes the remaining header messages that did not fit in the initial block,
     * starting after the specified initial size, to the provided buffer.
     * </p>
     *
     * @param initialSize the size of the initial message block
     * @param buffer      the ByteBuffer to write to
     */
    public void writeContinuationMessageBlockToBuffer(int initialSize, ByteBuffer buffer) {
        // Simulate object header data.
        int currentSize = 16;
        int i = 0;
        while (i < headerMessages.size()) {
            HdfMessage hdfMessage = headerMessages.get(i);
            currentSize += hdfMessage.getSizeMessageData() + 8;
            currentSize = (currentSize + 7) & ~7;
            i++;

            if (currentSize >= initialSize) {
                break;
            }
        }
        while (i < headerMessages.size()) {
            HdfMessage hdfMessage = headerMessages.get(i);
            hdfMessage.writeMessageToByteBuffer(buffer);
            i++;
        }
    }

    /**
     * Returns a string representation of the object header.
     * <p>
     * Includes the version, total number of header messages, reference count, header size,
     * and a detailed listing of all header messages.
     * </p>
     *
     * @return a string describing the object header
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("HdfObjectHeaderPrefixV1 {")
                .append(" Version: ").append(version)
                .append(" Total Header Messages: ").append(headerMessages.size())
                .append(" Object Reference Count: ").append(objectReferenceCount)
                .append(" Object Header Size: ").append(objectHeaderSize);
        // Parse header messages
        for (HdfMessage message : headerMessages) {
            String ms = message.toString();
            builder.append("\r\n").append(ms);
        }
        builder.append("}");

        return builder.toString();
    }

    /**
     * Finds a header message of the specified type.
     *
     * @param messageClass the class of the message to find
     * @param <T>          the type of the message
     * @return an Optional containing the message if found, or empty if not found
     */
    public <T extends HdfMessage> Optional<T> findMessageByType(Class<T> messageClass) {
        for (HdfMessage message : headerMessages) {
            if (messageClass.isInstance(message)) {
                return Optional.of(messageClass.cast(message)); // Avoids unchecked cast warning
            }
        }
        return Optional.empty();
    }

    public List<HdfMessage> getHeaderMessages() {
        return headerMessages;
    }
}