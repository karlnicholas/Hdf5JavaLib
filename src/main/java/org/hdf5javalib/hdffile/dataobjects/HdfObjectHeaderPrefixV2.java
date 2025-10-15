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
            long offset,
            long prefixSize
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
}
