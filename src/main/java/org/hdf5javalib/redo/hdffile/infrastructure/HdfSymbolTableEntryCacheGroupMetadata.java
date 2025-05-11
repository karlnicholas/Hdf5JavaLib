package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

public class HdfSymbolTableEntryCacheGroupMetadata implements HdfSymbolTableEntryCache {
    /** The cache type (0 for basic, 1 for additional B-Tree and heap offsets). */
    private final int cacheType = 1;
    /** The offset of the B-Tree for cache type 1 entries (null for cache type 0). */
    private final HdfBTreeV1 bTree;
    /** The offset of the local heap for cache type 1 entries (null for cache type 0). */
    private final HdfLocalHeap localHeap;

    public HdfSymbolTableEntryCacheGroupMetadata(HdfBTreeV1 bTree, HdfLocalHeap localHeap) {
        this.bTree = bTree;
        this.localHeap = localHeap;
    }

    public static HdfSymbolTableEntryCache readFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws Exception {
        HdfFixedPoint bTreeAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        fileChannel.position(bTreeAddress.getInstance(Long.class));
        HdfBTreeV1 bTreeV1 = HdfBTreeV1.readFromSeekableByteChannel(fileChannel, hdfDataFile);
        HdfFixedPoint localHeapAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        fileChannel.position(localHeapAddress.getInstance(Long.class));
        HdfLocalHeap localHeap = org.hdf5javalib.redo.hdffile.infrastructure.HdfLocalHeap.readFromSeekableByteChannel(fileChannel, hdfDataFile);
        return new HdfSymbolTableEntryCacheGroupMetadata(bTreeV1, localHeap);
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        writeFixedPointToBuffer(buffer, bTree.getOffset());
        writeFixedPointToBuffer(buffer, localHeap.getOffset());

    }

    @Override
    public int getCacheType() {
        return cacheType;
    }
}
