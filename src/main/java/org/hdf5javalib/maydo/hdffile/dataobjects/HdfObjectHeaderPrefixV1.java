package org.hdf5javalib.maydo.hdffile.dataobjects;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdfjava.AllocationType;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.hdfjava.HdfFileAllocation;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.ObjectHeaderContinuationMessage;
import org.hdf5javalib.maydo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

import static org.hdf5javalib.maydo.hdffile.dataobjects.messages.HdfMessage.HDF_MESSAGE_PREAMBLE_SIZE;

/**
 * Represents the version 1 object header prefix for an HDF5 data object.
 * <p>
 * The {@code HdfObjectHeaderPrefixV1} class encapsulates the metadata for an HDF5 data object's
 * header, including version, reference count, header size, and a list of header messages. It provides
 * methods to read the header from a file channel, write it to a byte channel or buffer, and manage
 * continuation messages for large headers.
 * </p>
 */
public class HdfObjectHeaderPrefixV1 extends HdfObjectHeaderPrefix {
    private static final int OBJECT_HEADER_PREFIX_HEADER_SIZE =16;
    private static final int OBJECT_HEADER_PREFIX_RESERVED_SIZE_1 =1;
    private static final int OBJECT_HEADER_PREFIX_RESERVED_SIZE_2 =4;
    /**
     * The version of the object header (1 byte).
     */
    private final int version;

    /**
     * The reference count for the object (4 bytes).
     */
    private final long objectReferenceCount;


    /**
     * Constructs an HdfObjectHeaderPrefixV1 with application-defined values.
     *
     * @param version              the version of the object header
     * @param objectReferenceCount the reference count for the object
     * @param objectHeaderSize     the size of the object header
     * @param headerMessages       the list of header messages
     */
    public HdfObjectHeaderPrefixV1(int version, long objectReferenceCount, long objectHeaderSize, List<HdfMessage> headerMessages,
                                   HdfDataFile hdfDataFile, AllocationType allocationType, String name, HdfFixedPoint offset
    ) {
        super(headerMessages, allocationType, name, offset, objectHeaderSize, hdfDataFile, OBJECT_HEADER_PREFIX_HEADER_SIZE);
        this.version = version;
        this.objectReferenceCount = objectReferenceCount;
    }

    protected static HdfObjectHeaderPrefixV1 readObjectHeader(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, String objectName, AllocationType allocationType) throws IOException {
        long offset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(OBJECT_HEADER_PREFIX_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN); // Buffer for the fixed-size header
        fileChannel.read(buffer);
        buffer.flip();

        // Parse Version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());

        // Reserved (1 byte, should be zero)
        buffer.position(buffer.position() + OBJECT_HEADER_PREFIX_RESERVED_SIZE_1);

        // Total Number of Header Messages (2 bytes, little-endian)
        int totalHeaderMessages = Short.toUnsignedInt(buffer.getShort());

        // Object Reference Count (4 bytes, little-endian)
        long objectReferenceCount = Integer.toUnsignedLong(buffer.getInt());

        // Object Header Size (4 bytes, little-endian)
        long objectHeaderSize = Integer.toUnsignedLong(buffer.getInt());

        // Reserved (4 bytes, should be zero)
        buffer.position(buffer.position() + OBJECT_HEADER_PREFIX_RESERVED_SIZE_2);

        List<HdfMessage> dataObjectHeaderMessages = new ArrayList<>(
                HdfMessage.readMessagesFromByteBuffer(fileChannel, objectHeaderSize, hdfDataFile, HdfMessage.V1_OBJECT_HEADER_READ_PREFIX)
        );

        for (HdfMessage hdfMessage : dataObjectHeaderMessages) {
            if (hdfMessage instanceof ObjectHeaderContinuationMessage objectHeaderContinuationMessage) {
                dataObjectHeaderMessages.addAll(HdfMessage.parseContinuationMessage(fileChannel, objectHeaderContinuationMessage, hdfDataFile, HdfMessage.V1_OBJECT_HEADER_READ_PREFIX));

                break;
            }
        }

        // Create the instance
        return new HdfObjectHeaderPrefixV1(version, objectReferenceCount, objectHeaderSize, dataObjectHeaderMessages,
                hdfDataFile, allocationType, objectName,
                HdfWriteUtils.hdfFixedPointFromValue(offset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()));
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
     * @throws IOException           if an I/O error occurs
     * @throws IllegalStateException if the buffer overflows
     */
    @Override
    public void writeAsGroupToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException {
        int currentSize = 16;
        int i = 0;
        while (i < headerMessages.size()) {
            HdfMessage hdfMessage = headerMessages.get(i);
            currentSize += hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE;
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
//        long rootGroupOffset = dataObjectAllocationRecord.getOffset().getInstance(Long.class);
//
//        seekableByteChannel.position(rootGroupOffset);
//        while (buffer.hasRemaining()) {
//            seekableByteChannel.write(buffer);
//        }
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
            currentSize += hdfMessage.getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE;
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
            builder.append("\r\n\t\t").append(ms);
        }
        builder.append("}");

        return builder.toString();
    }

}