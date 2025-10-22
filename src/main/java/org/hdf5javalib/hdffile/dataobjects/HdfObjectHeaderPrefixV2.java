package org.hdf5javalib.hdffile.dataobjects;

import org.hdf5javalib.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.time.Instant;
import java.util.List;

public class HdfObjectHeaderPrefixV2 extends HdfObjectHeaderPrefix {
    public static final byte[] OBJECT_HEADER_MESSAGE_SIGNATURE= {'O', 'H', 'D', 'R'};
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
            long offset
    ) {
        super(headerMessages,
                HdfWriteUtils.hdfFixedPointFromValue(offset, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength()),
            objectHeaderSize
//                , hdfDataFile, (int) prefixSize
        );
        this.flags = flags;
        this.checksum = checksum;
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;
        this.changeTime = changeTime;
        this.birthTime = birthTime;
        this.maxCompactAttributes = maxCompactAttributes;
        this.minDenseAttributes = minDenseAttributes;
    }
    @Override
    public void writeAsGroupToByteChannel(SeekableByteChannel seekableByteChannel) throws IOException {
        // not implemented
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("HdfObjectHeaderPrefixV2 {")
                .append(" Version: 2")
                .append(", Flags: ").append(flags)
                .append(", Checksum: ").append(checksum)
                .append(", Access Time: ").append(accessTime)
                .append(", Modification Time: ").append(modificationTime)
                .append(", Change Time: ").append(changeTime)
                .append(", Birth Time: ").append(birthTime)
                .append(", Max Compact Attributes: ").append(maxCompactAttributes)
                .append(", Min Dense Attributes: ").append(minDenseAttributes)
                .append(", Total Header Messages: ").append(headerMessages.size())
                .append(", Object Header Size: ").append(objectHeaderSize);

        // Parse header messages
        for (HdfMessage message : headerMessages) {
            String ms = message.toString();
            // Indent the nested message string
            builder.append("\r\n\t\t").append(ms.replace("\r\n", "\r\n\t\t"));
        }
        builder.append("\r\n}"); // Added newline for final brace readability

        return builder.toString();
    }
}
