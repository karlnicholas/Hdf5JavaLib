package org.hdf5javalib.hdffile.infrastructure.fractalheap;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.hdf5javalib.datatype.FixedPointDatatype.BIT_MULTIPLIER;

public class FractalHeap {
    private FractalHeapHeader header;
    private Block rootBlock;

    public FractalHeapHeader getHeader() {
        return header;
    }

    public Block getRootBlock() {
        return rootBlock;
    }

    public static FractalHeap read(SeekableByteChannel channel, long position, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        FixedPointDatatype sizeOfOffset = hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset();
        FixedPointDatatype sizeOfLength = hdfDataFile.getSuperblock().getFixedPointDatatypeForLength();
        channel.position(position);
        FractalHeapHeader header = readHeader(channel, hdfDataFile);
        HdfFixedPoint rootAddress = header.addressRootBlock;
        int nrows = header.currentNumRowsRootIndirectBlock;
        HdfFixedPoint filteredSize = sizeOfLength.undefined();
        long filterMask = 0;
        if (header.hasFilters && nrows == 0) {
            filteredSize = header.filteredRootDirectSize;
            filterMask = header.filterMaskRoot;
        }
        long expectedBlockOffset = 0;
        Block root;
        if (nrows == 0) {
            root = readDirectBlock(channel, header, rootAddress, sizeOfOffset, expectedBlockOffset, filteredSize, filterMask);
        } else {
            root = readIndirectBlock(channel, header, sizeOfOffset, sizeOfLength, rootAddress, expectedBlockOffset, -1, nrows);
        }
        FractalHeap heap = new FractalHeap();
        heap.header = header;
        heap.rootBlock = root;
        return heap;
    }

    private static FractalHeapHeader readHeader(SeekableByteChannel channel, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        FixedPointDatatype sizeOfOffset = hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset();
        FixedPointDatatype sizeOfLength = hdfDataFile.getSuperblock().getFixedPointDatatypeForLength();

        FractalHeapHeader h = new FractalHeapHeader();
        ByteBuffer signatureBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        channel.read(signatureBuffer);
        signatureBuffer.flip();
        h.signature = new String(signatureBuffer.array(), StandardCharsets.US_ASCII);
        if (!Objects.equals(h.signature, "FRHP")) {
            throw new IOException("Invalid signature");
        }
        int headerSize = getTotalBytesRead(sizeOfLength.getSize(), sizeOfLength.getSize());
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
        int bytesRead = channel.read(headerBuffer);
        if ( bytesRead != headerSize) {
            throw new IllegalStateException("Incorrect amount of bytes read: " + headerSize + " wanted but got " + bytesRead);
        }
        headerBuffer.flip();

        h.version = headerBuffer.get();
        h.heapIdLength = Short.toUnsignedInt(headerBuffer.getShort());
        h.ioFiltersEncodedLength = Short.toUnsignedInt(headerBuffer.getShort());
        h.flags = headerBuffer.get();
        h.sizeOfManagedObjects = Integer.toUnsignedLong(headerBuffer.getInt());
        h.nextHugeObjectId = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.v2BtreeAddress = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, headerBuffer);
        h.amountFreeSpaceManagedBlocks = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.addressManagedBlockFreeSpaceManager = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, headerBuffer);
        h.amountManagedSpaceHeap = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.amountAllocatedManagedSpaceHeap = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.offsetDirectBlockAllocationIteratorManagedSpace = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.numberManagedObjectsHeap = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.numberHugeObjectsHeap = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.totalSizeHugeObjectsHeap = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.numberTinyObjectsHeap = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.totalSizeTinyObjectsHeap = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.tableWidth = Short.toUnsignedInt(headerBuffer.getShort());
        h.startingBlockSize = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.maximumDirectBlockSize = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
        h.maximumHeapSize = Short.toUnsignedInt(headerBuffer.getShort());
        h.startingNumRowsRootIndirectBlock = Short.toUnsignedInt(headerBuffer.getShort());
        h.addressRootBlock = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, headerBuffer);
        h.currentNumRowsRootIndirectBlock = Short.toUnsignedInt(headerBuffer.getShort());

        h.hasFilters = h.ioFiltersEncodedLength > 0;
        h.checksumDirect = (h.flags & 0x02) != 0;
        if (h.hasFilters && h.currentNumRowsRootIndirectBlock == 0) {
            headerSize = sizeOfLength.getSize() + 4;
            headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
            bytesRead = channel.read(headerBuffer);
            if ( bytesRead != headerSize) {
                throw new IllegalStateException("Incorrect amount of bytes read: " + headerSize + " wanted but got " + bytesRead);
            }
            headerBuffer.flip();
            h.filteredRootDirectSize = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
            h.filterMaskRoot = Integer.toUnsignedLong(headerBuffer.getInt());
        }
        if (h.hasFilters) {
            headerBuffer = ByteBuffer.allocate(h.ioFiltersEncodedLength).order(ByteOrder.LITTLE_ENDIAN);
            bytesRead = channel.read(headerBuffer);
            if ( bytesRead != headerSize) {
                throw new IllegalStateException("Incorrect amount of bytes read: " + headerSize + " wanted but got " + bytesRead);
            }
            headerBuffer.flip();
            byte[] filterData = headerBuffer.array();
            h.filterPipeline = parseFilterPipeline(filterData);
        }
        headerSize = 4;
        headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
        bytesRead = channel.read(headerBuffer);
        if ( bytesRead != headerSize) {
            throw new IllegalStateException("Incorrect amount of bytes read: " + headerSize + " wanted but got " + bytesRead);
        }
        headerBuffer.flip();
        h.checksum = Integer.toUnsignedLong(headerBuffer.getInt());
        int offsetBytesSize = (h.maximumHeapSize + 7) / 8;
        h.offsetBytes = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                offsetBytesSize, 0, BIT_MULTIPLIER * offsetBytesSize, hdfDataFile);
        double logVal = Math.log((double) h.maximumDirectBlockSize.getInstance(Long.class) / h.startingBlockSize.getInstance(Long.class)) / Math.log(2);
        h.maxDblockRows = (int) Math.floor(logVal) + 1;
        return h;
    }

    /**
     * Calculates the total number of bytes read by the reader for the header.
     * Uses sizeOfOffsets and sizeOfLengths to determine the size of variable-length fields.
     *
     * @param sizeOfOffsets Number of bytes used for offset fields in readVariableLong.
     * @param sizeOfLengths Number of bytes used for length fields in readVariableLong.
     * @return Total bytes read as a long.
     */
    private static int getTotalBytesRead(int sizeOfOffsets, int sizeOfLengths) {
        int totalBytes = 0;

        // Fixed-size fields
        totalBytes += 1; // version (byte)
        totalBytes += 2; // heapIdLength (short)
        totalBytes += 2; // ioFiltersEncodedLength (short)
        totalBytes += 1; // flags (byte)
        totalBytes += 4; // sizeOfManagedObjects (int)
        totalBytes += 2; // tableWidth (short)
        totalBytes += 2; // maximumHeapSize (short)
        totalBytes += 2; // startingNumRowsRootIndirectBlock (short)
        totalBytes += 2; // currentNumRowsRootIndirectBlock (short)
//        totalBytes += 4; // checksum (int)
        // Total fixed: 20 bytes

        // Variable-length fields using sizeOfOffsets
        totalBytes += sizeOfOffsets; // nextHugeObjectId
        totalBytes += sizeOfOffsets; // v2BtreeAddress
        totalBytes += sizeOfOffsets; // addressManagedBlockFreeSpaceManager
        totalBytes += sizeOfOffsets; // addressRootBlock
        // Total: 4 * sizeOfOffsets

        // Variable-length fields using sizeOfLengths
        totalBytes += sizeOfLengths; // amountFreeSpaceManagedBlocks
        totalBytes += sizeOfLengths; // amountManagedSpaceHeap
        totalBytes += sizeOfLengths; // amountAllocatedManagedSpaceHeap
        totalBytes += sizeOfLengths; // offsetDirectBlockAllocationIteratorManagedSpace
        totalBytes += sizeOfLengths; // numberManagedObjectsHeap
        totalBytes += sizeOfLengths; // numberHugeObjectsHeap
        totalBytes += sizeOfLengths; // totalSizeHugeObjectsHeap
        totalBytes += sizeOfLengths; // numberTinyObjectsHeap
        totalBytes += sizeOfLengths; // totalSizeTinyObjectsHeap
        totalBytes += sizeOfLengths; // startingBlockSize
        totalBytes += sizeOfLengths; // maximumDirectBlockSize
        // Total: 11 * sizeOfLengths

        return totalBytes;
    }

    private static FilterPipeline parseFilterPipeline(byte[] data) throws IOException {
        FilterPipeline fp = new FilterPipeline();
        ByteBuffer bb = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        fp.version = bb.get() & 0xFF;
        if (fp.version == 1) {
            fp.filters = new ArrayList<>();
            int n = bb.get() & 0xFF;
            bb.get(); // reserved
            bb.getShort(); // reserved
            bb.getShort(); // reserved
            for (int i = 0; i < n; i++) {
                Filter f = new Filter();
                f.id = bb.getShort() & 0xFFFF;
                short nameLen = bb.getShort();
                f.flags = bb.getShort() & 0xFFFF;
                short c = bb.getShort();
                if (nameLen > 0) {
                    byte[] nameBytes = new byte[nameLen];
                    bb.get(nameBytes);
                    f.name = new String(nameBytes);
                    int pad = (nameLen % 4);
                    if (pad > 0) {
                        bb.position(bb.position() + (4 - pad));
                    }
                }
                f.clientData = new int[c];
                for (int j = 0; j < c; j++) {
                    f.clientData[j] = bb.getInt();
                }
                if (c % 2 != 0) {
                    bb.getInt(); // pad
                }
                fp.filters.add(f);
            }
        } else if (fp.version == 2) {
            fp.filters = new ArrayList<>();
            int n = bb.get() & 0xFF;
            bb.position(bb.position() + 6); // reserved
            for (int i = 0; i < n; i++) {
                Filter f = new Filter();
                f.id = bb.getShort() & 0xFFFF;
                short c = bb.getShort();
                f.clientData = new int[c];
                for (int j = 0; j < c; j++) {
                    f.clientData[j] = bb.getInt();
                }
                fp.filters.add(f);
            }
        } else {
            throw new IOException("Unsupported filter pipeline version");
        }
        return fp;
    }

    private static Block readDirectBlock(SeekableByteChannel channel, FractalHeapHeader header, HdfFixedPoint address, FixedPointDatatype sizeOfOffset, long expectedBlockOffset, HdfFixedPoint filteredSize, long filterMask) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (address.isUndefined()) {
            throw new IOException("Invalid direct block address");
        }
        channel.position(address.getInstance(Long.class));
        //
        int headerSize = 4 + 1 + sizeOfOffset.getSize() + header.offsetBytes.getSize() + (header.checksumDirect ? 4 : 0);
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
        int bytesRead = channel.read(headerBuffer);
        if ( headerSize != bytesRead)
            throw new IllegalStateException("Incorrect bytesRead: " + bytesRead + ": expected " + headerSize);
        headerBuffer.flip();
        String sig = new String(Arrays.copyOfRange(headerBuffer.array(), 0, 4), StandardCharsets.US_ASCII);
        if (!Objects.equals(sig, "FHDB")) {
            throw new IOException("Invalid direct block signature");
        }
        headerBuffer.position(4);
        byte version = headerBuffer.get();
        if (version != 0) {
            throw new IOException("Unsupported version");
        }
        HdfFixedPoint heapHeader = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, headerBuffer);
        if (heapHeader.isUndefined()) {
            throw new IOException("Invalid heap header address in direct block");
        }
        HdfFixedPoint blockOffset = HdfReadUtils.readHdfFixedPointFromBuffer(header.offsetBytes, headerBuffer);
        if (blockOffset.getInstance(Long.class) != expectedBlockOffset) {
            throw new IOException("Block offset mismatch");
        }
        headerBuffer.position(4 + 1 + sizeOfOffset.getSize()+header.offsetBytes.getSize());
        long checksum = 0;
        if (header.checksumDirect) {
            checksum = Integer.toUnsignedLong(headerBuffer.getInt()); // checksum, not verifying
        }
        long blockSize = getBlockSize(header, expectedBlockOffset);
        long dataSize;
        if (!filteredSize.isUndefined()) {
            dataSize = filteredSize.getInstance(Long.class);
        } else {
            dataSize = blockSize;
        }
        if (dataSize < 0) {
            throw new IOException("Invalid data size in direct block");
        }
        headerBuffer = ByteBuffer.allocate((int) dataSize).order(ByteOrder.LITTLE_ENDIAN);
        bytesRead = channel.read(headerBuffer);
        if ( bytesRead != dataSize)
            throw new IllegalStateException();
        headerBuffer.flip();

        byte[] data = headerBuffer.array();
        DirectBlock db = new DirectBlock();
        db.blockOffset = blockOffset.getInstance(Long.class);
        db.blockSize = blockSize;
        db.data = data;
        db.filterMask = filterMask;
        db.checksum = checksum;
        db.headerSize = headerSize;
        return db;
    }

    private static Block readIndirectBlock(SeekableByteChannel channel, FractalHeapHeader header, FixedPointDatatype sizeOfOffset, FixedPointDatatype sizeOfLength, HdfFixedPoint address, long expectedBlockOffset, long iblockSize, int passedNrows) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (address.isUndefined()) {
            throw new IOException("Invalid indirect block address");
        }
        channel.position(address.getInstance(Long.class));
        IndirectBlock ib = new IndirectBlock();
        int nrows;
        long startingBlockSize = header.startingBlockSize.getInstance(Long.class);
        if (iblockSize > 0) {
            long covered = 0;
            long current = startingBlockSize;
            int nr = 0;
            while (covered < iblockSize) {
                covered += (long) header.tableWidth * current;
                current *= 2;
                nr++;
            }
            nrows = (short) nr;
        } else {
            nrows = passedNrows;
        }
        int plusSize = 0;
        ib.nrows = nrows;
        for (short r = 0; r < nrows; r++) {
            for (int c = 0; c < header.tableWidth; c++) {
                plusSize += sizeOfOffset.getSize();
                boolean isDirect = r < header.maxDblockRows;
                if (header.hasFilters && isDirect) {
                    plusSize += sizeOfLength.getSize();
                    plusSize += 4;
                }
            }
        }
        plusSize += 4;  // checksum

        int headerSize = 4 + 1 + sizeOfOffset.getSize() + header.offsetBytes.getSize() + plusSize;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
        int bytesRead = channel.read(headerBuffer);
        if ( headerSize != bytesRead)
            throw new IllegalStateException("Incorrect bytesRead: " + bytesRead + ": expected " + headerSize);
        headerBuffer.flip();

        String sig = new String(Arrays.copyOfRange(headerBuffer.array(), 0, 4), StandardCharsets.US_ASCII);
        if (!Objects.equals(sig, "FHIB")) {
            throw new IOException("Invalid indirect block signature");
        }
        headerBuffer.position(4);
        byte version = headerBuffer.get();
        if (version != 0) {
            throw new IOException("Unsupported version");
        }
        HdfFixedPoint heapHeader = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, headerBuffer);
        if (heapHeader.isUndefined()) {
            throw new IOException("Invalid heap header address in indirect block");
        }
        HdfFixedPoint blockOffset = HdfReadUtils.readHdfFixedPointFromBuffer(header.offsetBytes, headerBuffer);
        if (blockOffset.getInstance(Long.class) != expectedBlockOffset) {
            throw new IOException("Block offset mismatch");
        }
        headerBuffer.position(4 + 1 + sizeOfOffset.getSize() + header.offsetBytes.getSize());
        ib.blockOffset = blockOffset.getInstance(Long.class);
        ib.children = new ArrayList<>();
        List<ChildInfo> childInfos = new ArrayList<>();
        long currentOffset = expectedBlockOffset; // expectedBlockOffset;
        for (short r = 0; r < nrows; r++) {
            long rowBlockSize = startingBlockSize * (1L << r);
            for (int c = 0; c < header.tableWidth; c++) {
                HdfFixedPoint childAddress = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, headerBuffer);
                HdfFixedPoint childFilteredSize = sizeOfOffset.undefined();
                long childFilterMask = 0;
                boolean isDirect = r < header.maxDblockRows;
                if (header.hasFilters && isDirect) {
                    childFilteredSize = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, headerBuffer);
                    childFilterMask = headerBuffer.getInt();
                }
                if (!childAddress.isUndefined()) {
                    ChildInfo info = new ChildInfo(childAddress, currentOffset, isDirect, childFilteredSize, childFilterMask, rowBlockSize);
                    childInfos.add(info);
                }
                currentOffset += rowBlockSize;
            }
        }
        ib.checksum = Integer.toUnsignedLong(headerBuffer.getInt());
        for (ChildInfo info : childInfos) {
            if (!info.address.isUndefined()) {
                Block child;
                if (info.isDirect) {
                    child = readDirectBlock(channel, header, info.address, sizeOfOffset, info.blockOffset, info.filteredSize, info.filterMask);
                } else {
                    child = readIndirectBlock(channel, header, sizeOfOffset, sizeOfLength, info.address, info.blockOffset, info.blockSize, (short) 0);
                }
                ib.children.add(child);
            }
        }
        return ib;
    }

    private static long getBlockSize(FractalHeapHeader header, long blockOffset) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        long startingBlockSize = header.startingBlockSize.getInstance(Long.class);
        if (blockOffset == 0 && header.currentNumRowsRootIndirectBlock == 0) {
            return startingBlockSize;
        }
        double arg = ((double) blockOffset / (header.tableWidth * header.startingBlockSize.getInstance(Long.class))) + 1;
        int row = (int) Math.floor(Math.log(arg) / Math.log(2));
        return startingBlockSize * (1L << row);
    }

    public byte[] getObject(ParsedHeapId heapId) {
        if ( rootBlock instanceof IndirectBlock ) {
            for(Block db: ((IndirectBlock) rootBlock).children) {
                DirectBlock dBlock = (DirectBlock) db;
                if ( heapId.offset >= dBlock.blockOffset && heapId.offset < (dBlock.blockOffset + dBlock.blockSize) ) {
                    int from = Math.toIntExact(heapId.offset - dBlock.blockOffset - dBlock.headerSize);
                    int to = from + heapId.length;
                    return Arrays.copyOfRange(dBlock.data, from, to);
                }
            }
        } else {
            DirectBlock dBlock = (DirectBlock) rootBlock;
            if ( heapId.offset >= dBlock.blockOffset && heapId.offset < (dBlock.blockOffset + dBlock.blockSize) ) {
                int from = Math.toIntExact(heapId.offset - dBlock.blockOffset - dBlock.headerSize);
                int to = from + heapId.length;
                return Arrays.copyOfRange(dBlock.data, from, to);
            } else {
                throw  new IllegalArgumentException("Unknown heap id " + heapId);
            }

        }
        throw  new IllegalArgumentException("Unknown heap id " + heapId);
    }

    public static class FractalHeapHeader {
        String signature;
        byte version;
        int heapIdLength;
        int ioFiltersEncodedLength;
        byte flags;
        long sizeOfManagedObjects;
        HdfFixedPoint nextHugeObjectId;
        HdfFixedPoint v2BtreeAddress;
        HdfFixedPoint amountFreeSpaceManagedBlocks;
        HdfFixedPoint addressManagedBlockFreeSpaceManager;
        HdfFixedPoint amountManagedSpaceHeap;
        HdfFixedPoint amountAllocatedManagedSpaceHeap;
        HdfFixedPoint offsetDirectBlockAllocationIteratorManagedSpace;
        HdfFixedPoint numberManagedObjectsHeap;
        HdfFixedPoint numberHugeObjectsHeap;
        HdfFixedPoint totalSizeHugeObjectsHeap;
        HdfFixedPoint numberTinyObjectsHeap;
        HdfFixedPoint totalSizeTinyObjectsHeap;
        int tableWidth;
        HdfFixedPoint startingBlockSize;
        HdfFixedPoint maximumDirectBlockSize;
        int maximumHeapSize;
        int startingNumRowsRootIndirectBlock;
        HdfFixedPoint addressRootBlock;
        int currentNumRowsRootIndirectBlock;
        HdfFixedPoint filteredRootDirectSize;
        long filterMaskRoot;
        FilterPipeline filterPipeline;
        long checksum;
        FixedPointDatatype offsetBytes;
        int maxDblockRows;
        boolean hasFilters;
        boolean checksumDirect;
    }

    public static class Filter {
        int id;
        String name;
        int flags;
        int[] clientData;
    }

    public static class FilterPipeline {
        int version;
        List<Filter> filters;
    }

    public static abstract class Block {
        long blockOffset;
    }

    public static class DirectBlock extends Block {
        public int headerSize;
        long blockSize;
        byte[] data;
        long filterMask;
        long checksum;
    }

    public static class IndirectBlock extends Block {
        int nrows;
        List<Block> children;
        long checksum;
    }

    private static class ChildInfo {
        HdfFixedPoint address;
        long blockOffset;
        boolean isDirect;
        HdfFixedPoint filteredSize;
        long filterMask;
        long blockSize;

        public ChildInfo(HdfFixedPoint address, long blockOffset, boolean isDirect, HdfFixedPoint filteredSize, long filterMask, long blockSize) {
            this.address = address;
            this.blockOffset = blockOffset;
            this.isDirect = isDirect;
            this.filteredSize = filteredSize;
            this.filterMask = filterMask;
            this.blockSize = blockSize;
        }
    }

//    public static long readVariableLong(byte[] bytes, int size) {
//        long val = 0;
//        for (int i = 0; i < size; i++) {
//            val |= (bytes[i] & 0xFFL) << (8 * i);
//        }
//        // If the most significant bit is set, check if all bits are 1 (undefined address)
//        if (size < 8 && (val & (1L << (size * 8 - 1))) != 0) {
//            long mask = (1L << (size * 8)) - 1;
//            if (val == mask) {
//                return -1L; // Undefined address
//            }
//        }
//        return val;
//    }
//
//    private static class HdfReader {
//        private final SeekableByteChannel channel;
//
//        public HdfReader(SeekableByteChannel channel) {
//            this.channel = channel;
//        }
//
//        public void seek(long pos) throws IOException {
//            channel.position(pos);
//        }
//
//        public byte readByte() throws IOException {
//            ByteBuffer bb = ByteBuffer.allocate(1);
//            channel.read(bb);
//            bb.flip();
//            return bb.get();
//        }
//
//        public short readShort() throws IOException {
//            ByteBuffer bb = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
//            channel.read(bb);
//            bb.flip();
//            return bb.getShort();
//        }
//
//        public int readInt() throws IOException {
//            ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
//            channel.read(bb);
//            bb.flip();
//            return bb.getInt();
//        }
//
//        public byte[] readBytes(int n) throws IOException {
//            ByteBuffer bb = ByteBuffer.allocate(n);
//            channel.read(bb);
//            bb.flip();
//            byte[] res = new byte[n];
//            bb.get(res);
//            return res;
//        }
//
//        public long readVariableLong(int size) throws IOException {
//            byte[] bytes = readBytes(size);
//            long val = 0;
//            for (int i = 0; i < size; i++) {
//                val |= (bytes[i] & 0xFFL) << (8 * i);
//            }
//            // If the most significant bit is set, check if all bits are 1 (undefined address)
//            if (size < 8 && (val & (1L << (size * 8 - 1))) != 0) {
//                long mask = (1L << (size * 8)) - 1;
//                if (val == mask) {
//                    return -1L; // Undefined address
//                }
//            }
//            return val;
//        }
//    }
}