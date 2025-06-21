package org.hdf5javalib.redo.hdffile.dataobjects;

import org.hdf5javalib.redo.hdffile.AllocationType;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.HdfFileAllocation;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.ObjectHeaderContinuationMessage;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class HdfObjectHeaderPrefixV2 extends HdfObjectHeaderPrefix {
    protected static final byte[] OBJECT_HEADER_MESSAGE_SIGNATURE= {'O', 'H', 'D', 'R'};
    private final byte flags;
    private final int checksum;
    private final Instant accessTime;
    private final Instant modificationTime;
    private final Instant changeTime;
    private final Instant birthTime;
    private final Integer maxCompactAttributes;
    private final Integer minDenseAttributes;

    public HdfObjectHeaderPrefixV2(
            byte flags,
            long objectHeaderSize,
            int checksum,
            Instant accessTime,
            Instant modificationTime,
            Instant changeTime,
            Instant birthTime,
            Integer maxCompactAttributes,
            Integer minDenseAttributes,
            List<HdfMessage> headerMessages,
            HdfDataFile hdfDataFile,
            AllocationType allocationType,
            String name,
            long offset,
            long prefixSize
    ) {
        super(headerMessages, allocationType, name,
                HdfWriteUtils.hdfFixedPointFromValue(offset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength()),
            objectHeaderSize, hdfDataFile, (int) prefixSize);
        this.flags = flags;
        this.checksum = checksum;
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;
        this.changeTime = changeTime;
        this.birthTime = birthTime;
        this.maxCompactAttributes = maxCompactAttributes;
        this.minDenseAttributes = minDenseAttributes;
    }
    static HdfObjectHeaderPrefixV2 readObjectHeader(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, String objectName, AllocationType allocationType) throws IOException {
        long offset = fileChannel.position();

        // --- 1. Read Signature, Version, and Flags ---
        // The first part of the header is 6 bytes: Signature (4) + Version (1) + Flags (1)
        ByteBuffer headerStartBuffer = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerStartBuffer);
        headerStartBuffer.flip();

        // Verify Signature ("OHDR")
        byte[] signatureBytes = new byte[4];
        headerStartBuffer.get(signatureBytes);
        String signature = new String(signatureBytes, "ASCII");
        if (!"OHDR".equals(signature)) {
            throw new IOException("Invalid HDF5 Object Header V2 signature. Expected 'OHDR', found '" + signature + "' at offset " + offset);
        }

        // Parse Version (must be 2)
        int version = Byte.toUnsignedInt(headerStartBuffer.get());
        if (version != 2) {
            throw new IOException("Unsupported Object Header version. Expected 2, found " + version);
        }

        // Parse Flags (1 byte)
        byte flags = headerStartBuffer.get();
        boolean timesPresent = (flags & 0b00100000) != 0;
        boolean attrPhaseChangePresent = (flags & 0b00010000) != 0;

        // --- 2. Read Optional Fields based on Flags ---
        Instant accessTime = null, modificationTime = null, changeTime = null, birthTime = null;
        if (timesPresent) {
            ByteBuffer timeBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN); // 4 * 4-byte timestamps
            fileChannel.read(timeBuffer);
            timeBuffer.flip();
            accessTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
            modificationTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
            changeTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
            birthTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
        }

        Integer maxCompactAttributes = null, minDenseAttributes = null;
        if (attrPhaseChangePresent) {
            ByteBuffer attrPhaseBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN); // 2 * 2-byte values
            fileChannel.read(attrPhaseBuffer);
            attrPhaseBuffer.flip();
            maxCompactAttributes = Short.toUnsignedInt(attrPhaseBuffer.getShort());
            minDenseAttributes = Short.toUnsignedInt(attrPhaseBuffer.getShort());
        }

        // --- 3. Read Size of Chunk #0 (Variable Size) --- // JAVA 17 OPTIMIZED
        int chunkSizeLength = 1 << (flags & 0b00000011); // 1, 2, 4, or 8 bytes
        ByteBuffer chunkSizeBytesBuffer = ByteBuffer.allocate(chunkSizeLength).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(chunkSizeBytesBuffer);
        chunkSizeBytesBuffer.flip();

        long sizeOfChunk0 = switch (chunkSizeLength) {
            case 1 -> Byte.toUnsignedInt(chunkSizeBytesBuffer.get());
            case 2 -> Short.toUnsignedInt(chunkSizeBytesBuffer.getShort());
            case 4 -> Integer.toUnsignedLong(chunkSizeBytesBuffer.getInt());
            case 8 -> chunkSizeBytesBuffer.getLong();
            default -> throw new IOException("Invalid chunk size length: " + chunkSizeLength);
        };


        // --- 4. Read Header Messages ---
        // The messages are located in a block of 'sizeOfChunk0' bytes, followed by a 4-byte checksum.
        long prefixSize = fileChannel.position() - offset;
        long messagesEndPosition = fileChannel.position() + sizeOfChunk0;

        List<HdfMessage> dataObjectHeaderMessages = new ArrayList<>(
                HdfMessage.readMessagesFromByteBuffer(fileChannel, sizeOfChunk0, hdfDataFile,
                        (flags & 0b00000100) > 0 ? HdfMessage.V2OBJECT_HEADER_READ_PREFIX_WITHORDER : HdfMessage.V2_OBJECT_HEADER_READ_PREFIX
                )
        );


        // After reading messages, the channel position might be before messagesEndPosition if a gap exists.
        // We must skip the gap to read the checksum.
        fileChannel.position(messagesEndPosition);

        // --- 5. Read Checksum ---
        ByteBuffer checksumBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(checksumBuffer);
        checksumBuffer.flip();
        int checksum = checksumBuffer.getInt();
        // Here you would typically verify the checksum against the header chunk data.

        // --- 6. Handle Continuation Messages ---
        // This logic is similar to V1, but you need to check the newly read messages.
        for (HdfMessage hdfMessage : dataObjectHeaderMessages) {
            if (hdfMessage instanceof ObjectHeaderContinuationMessage objectHeaderContinuationMessage) {
                // The continuation message points to the next chunk. You need a method to parse these.
                // A continuation chunk is NOT a full V2 header, it's just more messages.
                dataObjectHeaderMessages.addAll(HdfMessage.parseContinuationMessage(fileChannel, objectHeaderContinuationMessage, hdfDataFile,
                                (flags & 0b00000100) == 1 ? HdfMessage.V2OBJECT_HEADER_READ_PREFIX_WITHORDER : HdfMessage.V2_OBJECT_HEADER_READ_PREFIX
                        )
                );
                break; // Typically only one continuation message per chunk
            }
        }

        // --- 7. Create the V2 Header Prefix Instance ---
        return new HdfObjectHeaderPrefixV2(flags, sizeOfChunk0, checksum,
                accessTime, modificationTime, changeTime, birthTime,
                maxCompactAttributes, minDenseAttributes,
                dataObjectHeaderMessages, hdfDataFile, allocationType, objectName, offset, prefixSize);
    }

    @Override
    public void writeAsGroupToByteChannel(SeekableByteChannel seekableByteChannel, HdfFileAllocation fileAllocation) throws IOException {

    }
}
