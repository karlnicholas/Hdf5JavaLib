package org.hdf5javalib.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import org.hdf5javalib.file.metadata.HdfSuperblock;
import org.hdf5javalib.utils.HdfWriteUtils;

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
        FixedPointDatatype fixedPointDatatypeForOffset = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                8, (short) 0, (short) (8*8));
        FixedPointDatatype fixedPointDatatypeForLength = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                8, (short) 0, (short) (8*8));


        // 100320
        superblock = new HdfSuperblock(0, 0, 0, 0,
                4, 16,
                HdfWriteUtils.hdfFixedPointFromValue(0, fixedPointDatatypeForOffset),
                fixedPointDatatypeForOffset.undefined(),
                // this offset of the end of the file. will need to be updated later.
                HdfWriteUtils.hdfFixedPointFromValue(0, fixedPointDatatypeForOffset),
                fixedPointDatatypeForOffset.undefined(),
                new HdfSymbolTableEntry(
                        HdfWriteUtils.hdfFixedPointFromValue(0, fixedPointDatatypeForOffset),
                        // HdfFixedPoint.of(bufferAllocation.getObjectHeaderPrefixAddress()),
                        HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getObjectHeaderPrefixOffset(), fixedPointDatatypeForOffset),
                        // HdfFixedPoint.of(bufferAllocation.getBtreeAddress()),
                        HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getBtreeOffset(), fixedPointDatatypeForOffset),
                        // HdfFixedPoint.of(bufferAllocation.getLocalHeapAddress())));
                        HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getLocalHeapOffset(), fixedPointDatatypeForOffset)),
                this, fixedPointDatatypeForOffset, fixedPointDatatypeForLength);

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
        return rootGroup.createDataSet(this, datasetName, hdfDatatype, dataSpaceMessage);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        rootGroup.close();
        long endOfFileAddress = fileAllocation.getEndOfFileOffset();
        superblock.setEndOfFileAddress(
                HdfWriteUtils.hdfFixedPointFromValue(endOfFileAddress, getFixedPointDatatypeForOffset()));

        // write super block
        log.debug("{}", superblock);
        superblock.writeToFileChannel(seekableByteChannel);

        // write root group, writes all dataset and snod allocations as well.
        log.debug("{}", rootGroup);
        rootGroup.writeToFileChannel(seekableByteChannel);

        getGlobalHeap().writeToFileChannel(seekableByteChannel);

        closed = true;
    }

    @Override
    public FixedPointDatatype getFixedPointDatatypeForOffset() {
        return superblock.getFixedPointDatatypeForOffset();
    }

    @Override
    public FixedPointDatatype getFixedPointDatatypeForLength() {
        return superblock.getFixedPointDatatypeForLength();
    }
}
