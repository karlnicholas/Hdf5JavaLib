package org.hdf5javalib.maydo.hdffile.infrastructure;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.maydo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import static org.hdf5javalib.maydo.utils.HdfWriteUtils.writeFixedPointToBuffer;

public class HdfSymbolTableEntryCacheGroupMetadata implements HdfSymbolTableEntryCache {
    /**
     * The cache type (0 for basic, 1 for additional B-Tree and heap offsets).
     */
    private final int cacheType = 1;
    /**
     * The offset of the B-Tree for cache type 1 entries (null for cache type 0).
     */
//    private final HdfBTreeV1 bTree;
//    /** The offset of the local heap for cache type 1 entries (null for cache type 0). */
//    private final HdfLocalHeap localHeap;
    private final HdfGroup group;

    public HdfSymbolTableEntryCacheGroupMetadata(String groupName, HdfObjectHeaderPrefix objectHeader, HdfBTreeV1 bTree, HdfLocalHeap localHeap, HdfDataFile hdfDataFile) {
        group = new HdfGroup(groupName, objectHeader, bTree, localHeap,
//                new LinkedHashMap<>(),
                hdfDataFile);
    }

    public static HdfSymbolTableEntryCache readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            HdfObjectHeaderPrefix objectHeader,
            String objectName
    ) throws Exception {
        // reading for group.
        HdfFixedPoint bTreeAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        HdfFixedPoint localHeapAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        long savedPosition = fileChannel.position();

        fileChannel.position(localHeapAddress.getInstance(Long.class));
        HdfLocalHeap localHeap = HdfLocalHeap.readFromSeekableByteChannel(fileChannel, hdfDataFile, objectName);

        fileChannel.position(bTreeAddress.getInstance(Long.class));
        HdfBTreeV1 bTreeV1 = HdfBTreeV1.readFromSeekableByteChannel(fileChannel, hdfDataFile, localHeap, objectName);
        fileChannel.position(savedPosition);
        return new HdfSymbolTableEntryCacheGroupMetadata(objectName, objectHeader, bTreeV1, localHeap, hdfDataFile);
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        writeFixedPointToBuffer(buffer, group.getBTree().getAllocationRecord().getOffset());
        writeFixedPointToBuffer(buffer, group.getLocalHeap().getAllocationRecord().getOffset());

    }

    @Override
    public String toString() {
        return "HdfSymbolTableEntryCacheGroupMetadata{" + "cacheType=" + cacheType +
                "group=" + group +
                "}";
    }

    @Override
    public int getCacheType() {
        return cacheType;
    }

    @Override
    public HdfObjectHeaderPrefix getObjectHeader() {
        return group.getObjectHeader();
    }

    public HdfBTreeV1 getBtree() {
        return group.getBTree();
    }

    public HdfGroup getGroup() {
        return group;
    }
}
