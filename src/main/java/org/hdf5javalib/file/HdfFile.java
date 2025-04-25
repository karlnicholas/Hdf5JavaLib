package org.hdf5javalib.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import org.hdf5javalib.file.metadata.HdfSuperblock;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

@Getter
@Slf4j
public class HdfFile implements Closeable, HdfDataFile {
    // initial setup without Dataset
    private final HdfSuperblock superblock;
    private final HdfGroup rootGroup;
    private final HdfGlobalHeap globalHeap;
    private final HdfFileAllocation fileAllocation;
    private final SeekableByteChannel seekableByteChannel;
    private boolean closed;

    public HdfFile(SeekableByteChannel seekableByteChannel) {
        closed = false;
        this.seekableByteChannel = seekableByteChannel;
        // this.globalHeap = new HdfGlobalHeap(bufferAllocation::getGlobalHeapAddress);
        this.fileAllocation = new HdfFileAllocation();
        this.globalHeap = new HdfGlobalHeap(this);

        // 100320
        superblock = new HdfSuperblock(0, 0, 0, 0,
                (short)8, (short)8,
                4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                // HdfFixedPoint.of(bufferAllocation.getDataAddress()),
                // this offset of the end of the file. will need to be updated later.
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                new HdfSymbolTableEntry(
                        HdfFixedPoint.of(0),
                        // HdfFixedPoint.of(bufferAllocation.getObjectHeaderPrefixAddress()),
                        HdfFixedPoint.of(fileAllocation.getObjectHeaderPrefixOffset()),
                        // HdfFixedPoint.of(bufferAllocation.getBtreeAddress()),
                        HdfFixedPoint.of(fileAllocation.getBtreeOffset()),
                        // HdfFixedPoint.of(bufferAllocation.getLocalHeapAddress())));
                        HdfFixedPoint.of(fileAllocation.getLocalHeapOffset())), this);

        // rootGroup = new HdfGroup(this, "", bufferAllocation.getBtreeAddress(), bufferAllocation.getLocalHeapAddress());
        rootGroup = new HdfGroup(this, "", fileAllocation.getBtreeOffset(), fileAllocation.getLocalHeapOffset());
    }

    /**
     * by default, the root group.
     * @param datasetName String
     * @param hdfDatatype HdfDatatype
     * @param dataSpaceMessage DataspaceMessage
     * @return HdfDataSet
     */
    public HdfDataSet createDataSet(String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        hdfDatatype.setGlobalHeap(globalHeap);
        // return rootGroup.createDataSet(datasetName, hdfDatatype, dataSpaceMessage, bufferAllocation.getDataGroupAddress());
        return rootGroup.createDataSet(this, datasetName, hdfDatatype, dataSpaceMessage);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        rootGroup.close();
        // long endOfFileAddress = bufferAllocation.getDataAddress();
        long endOfFileAddress = fileAllocation.getEndOfFileOffset();
        superblock.setEndOfFileAddress(HdfFixedPoint.of(endOfFileAddress));

        // write super block
        log.debug("{}", superblock);
        superblock.writeToFileChannel(seekableByteChannel);

        // write root group, writes all dataset and snod allocations as well.
        log.debug("{}", rootGroup);
        rootGroup.writeToFileChannel(seekableByteChannel);

        getGlobalHeap().writeToFileChannel(seekableByteChannel);

        closed = true;
    }
}
