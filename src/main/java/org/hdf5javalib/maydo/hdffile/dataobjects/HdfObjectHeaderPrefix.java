package org.hdf5javalib.maydo.hdffile.dataobjects;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdffile.AllocationRecord;
import org.hdf5javalib.maydo.hdffile.AllocationType;
import org.hdf5javalib.maydo.hdffile.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.HdfFileAllocation;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DataLayoutMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.ObjectHeaderContinuationMessage;
import org.hdf5javalib.maydo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public abstract class HdfObjectHeaderPrefix {
    protected final AllocationRecord dataObjectAllocationRecord;
    protected final AllocationRecord dataObjectContinuationAllocationRecord;
    protected final AllocationRecord dataAllocationRecord;
    /**
     * The size of the object header (4 bytes).
     */
    protected final long objectHeaderSize;

    /**
     * The list of header messages associated with the object.
     */
    protected final List<HdfMessage> headerMessages;

    protected HdfObjectHeaderPrefix(
            List<HdfMessage> headerMessages,
            AllocationType allocationType,
            String name,
            HdfFixedPoint offset,
            long objectHeaderSize,
            HdfDataFile hdfDataFile,
            int OBJECT_HREADER_PREFIX_HEADER_SIZE
    ) {
        this.headerMessages = headerMessages;
        this.objectHeaderSize = objectHeaderSize;
        this.dataObjectAllocationRecord = new AllocationRecord(
                allocationType,
                name + ":Object Header",
                offset,
                HdfWriteUtils.hdfFixedPointFromValue(
                        objectHeaderSize + OBJECT_HREADER_PREFIX_HEADER_SIZE,
                        hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength()
                ), hdfDataFile.getFileAllocation()
        );
        this.dataObjectContinuationAllocationRecord = findMessageByType(ObjectHeaderContinuationMessage.class)
                .map(cm->
            new AllocationRecord(
            AllocationType.DATASET_HEADER_CONTINUATION,
            name+ ":Header Continuation",
            cm.getContinuationOffset(),
                    cm.getContinuationSize(),
                    hdfDataFile.getFileAllocation())
                ).orElse(null);
        this.dataAllocationRecord = findMessageByType(DataLayoutMessage.class)
                .map(dlm->
            dlm.getDataAddress().isUndefined() ? null :
            new AllocationRecord(
            AllocationType.DATASET_DATA,
            name+ ":Data",
            dlm.getDataAddress(),
                    dlm.getDimensionSizes()[0],
                    hdfDataFile.getFileAllocation())
                ).orElse(null);

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
     * @throws IOException              if an I/O error occurs
     * @throws IllegalArgumentException if reserved fields are non-zero
     */
    public static HdfObjectHeaderPrefix readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            String objectName,
            AllocationType allocationType
    ) throws IOException {
        long offset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN); // Buffer for the fixed-size header
        fileChannel.read(buffer);
        buffer.flip();

        // Parse Version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());
        if ( version == 1 ) {
            fileChannel.position(offset);
            return HdfObjectHeaderPrefixV1.readObjectHeader(fileChannel, hdfDataFile, objectName, allocationType);
        } else {
            buffer.rewind();
            byte[] signature = new byte[HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE.length];
            buffer.get(signature);
            if (Arrays.compare(signature, HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE) != 0) {
                throw new IllegalStateException("Object header signature mismatch");
            }
            fileChannel.position(offset);
            return HdfObjectHeaderPrefixV2.readObjectHeader(fileChannel, hdfDataFile, objectName, allocationType);
        }
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

    public AllocationRecord getDataObjectAllocationRecord() {
        return dataObjectAllocationRecord;
    }

    public abstract void writeAsGroupToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException;

    public void writeInitialMessageBlockToBuffer(ByteBuffer buffer) {
    }

    public void writeContinuationMessageBlockToBuffer(Integer instance, ByteBuffer buffer) {

    }
}
