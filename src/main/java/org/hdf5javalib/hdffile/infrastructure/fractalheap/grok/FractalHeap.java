package org.hdf5javalib.hdffile.infrastructure.fractalheap.grok;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class FractalHeap {
    private FractalHeapHeader header;
    private Block rootBlock;

    public FractalHeapHeader getHeader() {
        return header;
    }

    public Block getRootBlock() {
        return rootBlock;
    }

    public static FractalHeap read(SeekableByteChannel channel, long position, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        HdfReader reader = new HdfReader(channel);
        reader.seek(position);
        FractalHeapHeader header = readHeader(reader, sizeOfOffsets, sizeOfLengths);
        long rootAddress = header.addressRootBlock;
        short nrows = header.currentNumRowsRootIndirectBlock;
        long filteredSize = -1;
        int filterMask = 0;
        if (header.hasFilters && nrows == 0) {
            filteredSize = header.filteredRootDirectSize;
            filterMask = header.filterMaskRoot;
        }
        long expectedBlockOffset = 0;
        Block root;
        if (nrows == 0) {
            root = readDirectBlock(reader, header, rootAddress, expectedBlockOffset, filteredSize, filterMask);
        } else {
            root = readIndirectBlock(reader, header, rootAddress, expectedBlockOffset, -1, nrows);
        }
        FractalHeap heap = new FractalHeap();
        heap.header = header;
        heap.rootBlock = root;
        return heap;
    }

    private static FractalHeapHeader readHeader(HdfReader reader, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        FractalHeapHeader h = new FractalHeapHeader();
        h.signature = new String(reader.readBytes(4));
        if (!Objects.equals(h.signature, "FRHP")) {
            throw new IOException("Invalid signature");
        }
        h.version = reader.readByte();
        h.heapIdLength = reader.readShort();
        h.ioFiltersEncodedLength = reader.readShort();
        h.flags = reader.readByte();
        h.sizeOfManagedObjects = reader.readInt();
        h.nextHugeObjectId = reader.readVariableLong(sizeOfLengths);
        h.v2BtreeAddress = reader.readVariableLong(sizeOfOffsets);
        h.amountFreeSpaceManagedBlocks = reader.readVariableLong(sizeOfLengths);
        h.addressManagedBlockFreeSpaceManager = reader.readVariableLong(sizeOfOffsets);
        h.amountManagedSpaceHeap = reader.readVariableLong(sizeOfLengths);
        h.amountAllocatedManagedSpaceHeap = reader.readVariableLong(sizeOfLengths);
        h.offsetDirectBlockAllocationIteratorManagedSpace = reader.readVariableLong(sizeOfLengths);
        h.numberManagedObjectsHeap = reader.readVariableLong(sizeOfLengths);
        h.numberHugeObjectsHeap = reader.readVariableLong(sizeOfLengths);
        h.totalSizeHugeObjectsHeap = reader.readVariableLong(sizeOfLengths);
        h.numberTinyObjectsHeap = reader.readVariableLong(sizeOfLengths);
        h.totalSizeTinyObjectsHeap = reader.readVariableLong(sizeOfLengths);
        h.tableWidth = reader.readShort();
        h.startingBlockSize = reader.readVariableLong(sizeOfLengths);
        h.maximumDirectBlockSize = reader.readVariableLong(sizeOfLengths);
        h.maximumHeapSize = reader.readShort();
        h.startingNumRowsRootIndirectBlock = reader.readShort();
        h.addressRootBlock = reader.readVariableLong(sizeOfOffsets);
        h.currentNumRowsRootIndirectBlock = reader.readShort();
        h.hasFilters = h.ioFiltersEncodedLength > 0;
        h.checksumDirect = (h.flags & 0x02) != 0;
        if (h.hasFilters && h.currentNumRowsRootIndirectBlock == 0) {
            h.filteredRootDirectSize = reader.readVariableLong(sizeOfLengths);
            h.filterMaskRoot = reader.readInt();
        }
        if (h.hasFilters) {
            byte[] filterData = reader.readBytes(h.ioFiltersEncodedLength);
            h.filterPipeline = parseFilterPipeline(filterData);
        }
        h.checksum = reader.readInt();
        h.sizeOfOffsets = sizeOfOffsets;
        h.sizeOfLengths = sizeOfLengths;
        h.offsetBytes = (h.maximumHeapSize + 7) / 8;
        double logVal = Math.log((double) h.maximumDirectBlockSize / h.startingBlockSize) / Math.log(2);
        h.maxDblockRows = (int) Math.floor(logVal) + 1;
        return h;
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

    private static Block readDirectBlock(HdfReader reader, FractalHeapHeader header, long address, long expectedBlockOffset, long filteredSize, int filterMask) throws IOException {
        long undefined = -1L;
        if (address == undefined) {
            throw new IOException("Invalid direct block address");
        }
        reader.seek(address);
        String sig = new String(reader.readBytes(4));
        if (!Objects.equals(sig, "FHDB")) {
            throw new IOException("Invalid direct block signature");
        }
        byte version = reader.readByte();
        if (version != 0) {
            throw new IOException("Unsupported version");
        }
        long heapHeader = reader.readVariableLong(header.sizeOfOffsets);
        if (heapHeader == undefined) {
            throw new IOException("Invalid heap header address in direct block");
        }
        long blockOffset = reader.readVariableLong(header.offsetBytes);
        if (blockOffset != expectedBlockOffset) {
            throw new IOException("Block offset mismatch");
        }
        int checksum = 0;
        if (header.checksumDirect) {
            checksum = reader.readInt(); // checksum, not verifying
        }
        int headerSize = 4 + 1 + header.sizeOfOffsets + header.offsetBytes + (header.checksumDirect ? 4 : 0);
        long blockSize = getBlockSize(header, expectedBlockOffset);
        long dataSize;
//        if (filteredSize != -1) {
//            dataSize = filteredSize - headerSize;
//        } else {
//            dataSize = blockSize - headerSize;
//        }
        if (filteredSize != -1) {
            dataSize = filteredSize;
        } else {
            dataSize = blockSize;
        }
        if (dataSize < 0) {
            throw new IOException("Invalid data size in direct block");
        }
        byte[] data = reader.readBytes((int) dataSize);
        DirectBlock db = new DirectBlock();
        db.blockOffset = blockOffset;
        db.blockSize = blockSize;
        db.data = data;
        db.filterMask = filterMask;
        db.checksum = checksum;
        return db;
    }

    private static Block readIndirectBlock(HdfReader reader, FractalHeapHeader header, long address, long expectedBlockOffset, long iblockSize, short passedNrows) throws IOException {
        long undefined = -1L;
        if (address == undefined) {
            throw new IOException("Invalid indirect block address");
        }
        reader.seek(address);
        String sig = new String(reader.readBytes(4));
        if (!Objects.equals(sig, "FHIB")) {
            throw new IOException("Invalid indirect block signature");
        }
        byte version = reader.readByte();
        if (version != 0) {
            throw new IOException("Unsupported version");
        }
        long heapHeader = reader.readVariableLong(header.sizeOfOffsets);
        if (heapHeader == undefined) {
            throw new IOException("Invalid heap header address in indirect block");
        }
        long blockOffset = reader.readVariableLong(header.offsetBytes);
        if (blockOffset != expectedBlockOffset) {
            throw new IOException("Block offset mismatch");
        }
        IndirectBlock ib = new IndirectBlock();
        ib.blockOffset = blockOffset;
        ib.children = new ArrayList<>();
        short nrows;
        if (iblockSize > 0) {
            long covered = 0;
            long current = header.startingBlockSize;
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
        ib.nrows = nrows;
        List<ChildInfo> childInfos = new ArrayList<>();
        long currentOffset = expectedBlockOffset;
        for (short r = 0; r < nrows; r++) {
            long rowBlockSize = header.startingBlockSize * (1L << r);
            for (int c = 0; c < header.tableWidth; c++) {
                long childAddress = reader.readVariableLong(header.sizeOfOffsets);
                long childFilteredSize = -1;
                int childFilterMask = 0;
                boolean isDirect = r < header.maxDblockRows;
                if (header.hasFilters && isDirect) {
                    childFilteredSize = reader.readVariableLong(header.sizeOfLengths);
                    childFilterMask = reader.readInt();
                }
                if (childAddress != undefined) {
                    long childBlockOffset = currentOffset;
                    ChildInfo info = new ChildInfo(childAddress, childBlockOffset, isDirect, childFilteredSize, childFilterMask, rowBlockSize);
                    childInfos.add(info);
                }
                currentOffset += rowBlockSize;
            }
        }
        ib.checksum = reader.readInt();
        for (ChildInfo info : childInfos) {
            if (info.address != undefined) {
                Block child;
                if (info.isDirect) {
                    child = readDirectBlock(reader, header, info.address, info.blockOffset, info.filteredSize, info.filterMask);
                } else {
                    child = readIndirectBlock(reader, header, info.address, info.blockOffset, info.blockSize, (short) 0);
                }
                ib.children.add(child);
            }
        }
        return ib;
    }

    private static long getBlockSize(FractalHeapHeader header, long blockOffset) {
        if (blockOffset == 0 && header.currentNumRowsRootIndirectBlock == 0) {
            return header.startingBlockSize;
        }
        double arg = ((double) blockOffset / (header.tableWidth * header.startingBlockSize)) + 1;
        int row = (int) Math.floor(Math.log(arg) / Math.log(2));
        return header.startingBlockSize * (1L << row);
    }

    public byte[] getObject(ParsedHeapId heapId) {
        if ( rootBlock instanceof IndirectBlock ) {
            for(Block db: ((IndirectBlock) rootBlock).children) {
                DirectBlock dBlock = (DirectBlock) db;
                if ( heapId.offset >= dBlock.blockOffset && heapId.offset < (dBlock.blockOffset + dBlock.blockSize) ) {
                    int from = Math.toIntExact(heapId.offset - dBlock.blockOffset - 22);
                    int to = from + heapId.length;
                    return Arrays.copyOfRange(dBlock.data, from, to);
                }
            }
        }
        throw  new IllegalArgumentException("Unknown heap id " + heapId);
    }

    public static class FractalHeapHeader {
        String signature;
        byte version;
        short heapIdLength;
        short ioFiltersEncodedLength;
        byte flags;
        int sizeOfManagedObjects;
        long nextHugeObjectId;
        long v2BtreeAddress;
        long amountFreeSpaceManagedBlocks;
        long addressManagedBlockFreeSpaceManager;
        long amountManagedSpaceHeap;
        long amountAllocatedManagedSpaceHeap;
        long offsetDirectBlockAllocationIteratorManagedSpace;
        long numberManagedObjectsHeap;
        long numberHugeObjectsHeap;
        long totalSizeHugeObjectsHeap;
        long numberTinyObjectsHeap;
        long totalSizeTinyObjectsHeap;
        short tableWidth;
        long startingBlockSize;
        long maximumDirectBlockSize;
        short maximumHeapSize;
        short startingNumRowsRootIndirectBlock;
        long addressRootBlock;
        short currentNumRowsRootIndirectBlock;
        long filteredRootDirectSize;
        int filterMaskRoot;
        FilterPipeline filterPipeline;
        long checksum;
        int sizeOfOffsets;
        int sizeOfLengths;
        int offsetBytes;
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
        long blockSize;
        byte[] data;
        int filterMask;
        int checksum;
    }

    public static class IndirectBlock extends Block {
        short nrows;
        List<Block> children;
        int checksum;
    }

    private static class ChildInfo {
        long address;
        long blockOffset;
        boolean isDirect;
        long filteredSize;
        int filterMask;
        long blockSize;

        public ChildInfo(long address, long blockOffset, boolean isDirect, long filteredSize, int filterMask, long blockSize) {
            this.address = address;
            this.blockOffset = blockOffset;
            this.isDirect = isDirect;
            this.filteredSize = filteredSize;
            this.filterMask = filterMask;
            this.blockSize = blockSize;
        }
    }

    private static class HdfReader {
        private final SeekableByteChannel channel;

        public HdfReader(SeekableByteChannel channel) {
            this.channel = channel;
        }

        public void seek(long pos) throws IOException {
            channel.position(pos);
        }

        public byte readByte() throws IOException {
            ByteBuffer bb = ByteBuffer.allocate(1);
            channel.read(bb);
            bb.flip();
            return bb.get();
        }

        public short readShort() throws IOException {
            ByteBuffer bb = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
            channel.read(bb);
            bb.flip();
            return bb.getShort();
        }

        public int readInt() throws IOException {
            ByteBuffer bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            channel.read(bb);
            bb.flip();
            return bb.getInt();
        }

        public byte[] readBytes(int n) throws IOException {
            ByteBuffer bb = ByteBuffer.allocate(n);
            channel.read(bb);
            bb.flip();
            byte[] res = new byte[n];
            bb.get(res);
            return res;
        }

        public long readVariableLong(int size) throws IOException {
            byte[] bytes = readBytes(size);
            long val = 0;
            for (int i = 0; i < size; i++) {
                val |= (bytes[i] & 0xFFL) << (8 * i);
            }
            // If the most significant bit is set, check if all bits are 1 (undefined address)
            if (size < 8 && (val & (1L << (size * 8 - 1))) != 0) {
                long mask = (1L << (size * 8)) - 1;
                if (val == mask) {
                    return -1L; // Undefined address
                }
            }
            return val;
        }
    }
}