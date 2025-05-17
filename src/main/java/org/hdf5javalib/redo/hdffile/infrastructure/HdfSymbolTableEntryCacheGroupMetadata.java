package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.HdfDataFile;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfGroup;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.redo.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.LinkedHashMap;

import static org.hdf5javalib.redo.utils.HdfWriteUtils.writeFixedPointToBuffer;

public class HdfSymbolTableEntryCacheGroupMetadata implements HdfSymbolTableEntryCache {
    /** The cache type (0 for basic, 1 for additional B-Tree and heap offsets). */
    private final int cacheType = 1;
    /** The offset of the B-Tree for cache type 1 entries (null for cache type 0). */
//    private final HdfBTreeV1 bTree;
//    /** The offset of the local heap for cache type 1 entries (null for cache type 0). */
//    private final HdfLocalHeap localHeap;
    private final HdfGroup group;

    public HdfSymbolTableEntryCacheGroupMetadata(String groupName, HdfObjectHeaderPrefixV1 objectHeader, HdfBTreeV1 bTree, HdfLocalHeap localHeap, HdfDataFile hdfDataFile) {
        group = new HdfGroup(groupName, objectHeader, bTree, localHeap, new LinkedHashMap<>(), hdfDataFile);
    }

    public static HdfSymbolTableEntryCache readFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            HdfFixedPoint linkNameOffset,
            HdfObjectHeaderPrefixV1 objectHeader
    ) throws Exception {
        // reading for group.
        HdfFixedPoint localHeapAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        fileChannel.position(localHeapAddress.getInstance(Long.class));
        HdfLocalHeap localHeap = HdfLocalHeap.readFromSeekableByteChannel(fileChannel, hdfDataFile, linkNameOffset);

        String groupName = localHeap.parseStringAtOffset(linkNameOffset).toString();

        HdfFixedPoint bTreeAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        fileChannel.position(bTreeAddress.getInstance(Long.class));
        HdfBTreeV1 bTreeV1 = HdfBTreeV1.readFromSeekableByteChannel(fileChannel, hdfDataFile, groupName, localHeap);

        return new HdfSymbolTableEntryCacheGroupMetadata(groupName, objectHeader, bTreeV1, localHeap, hdfDataFile);
    }

    @Override
    public void writeToBuffer(ByteBuffer buffer) {
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        writeFixedPointToBuffer(buffer, group.getBTree().getOffset());
        writeFixedPointToBuffer(buffer, group.getLocalHeap().getOffset());

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HdfSymbolTableEntryCacheGroupMetadata{");
        sb.append("cacheType=").append(cacheType);
        sb.append("group=").append(group);
        return sb.append("}").toString();
    }

    @Override
    public HdfObjectHeaderPrefixV1 getObjectHeader() {
        return group.getObjectHeader();
    }

    public HdfBTreeV1 getBtree() {
        return group.getBTree();
    }
}
