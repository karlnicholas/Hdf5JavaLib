package org.hdf5javalib.hdffile.dataobjects;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfFileAllocation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Optional;

public abstract class HdfObjectHeaderPrefix {
    /**
     * The size of the object header (4 bytes).
     */
    protected final long objectHeaderSize;
    protected final HdfFixedPoint offset;

    /**
     * The list of header messages associated with the object.
     */
    protected final List<HdfMessage> headerMessages;


    protected HdfObjectHeaderPrefix(
            List<HdfMessage> headerMessages,
            HdfFixedPoint offset,
            long objectHeaderSize,
            HdfDataFile hdfDataFile,
            int OBJECT_HREADER_PREFIX_HEADER_SIZE
    ) {
        this.headerMessages = headerMessages;
        this.objectHeaderSize = objectHeaderSize;
        this.offset = offset;
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

    public HdfFixedPoint getOffset() { return offset; }

    public List<HdfMessage> getHeaderMessages() {
        return headerMessages;
    }

    public abstract void writeAsGroupToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException;

    public void writeInitialMessageBlockToBuffer(ByteBuffer buffer) {
    }

    public void writeContinuationMessageBlockToBuffer(Integer instance, ByteBuffer buffer) {

    }
}
