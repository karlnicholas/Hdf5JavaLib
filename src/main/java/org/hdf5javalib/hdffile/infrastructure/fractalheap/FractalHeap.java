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
        fp.filters = new ArrayList<>();
        int n; // Number of filters

        switch (fp.version) {
            case 1:
                n = bb.get() & 0xFF;
                // Skip reserved bytes
                bb.position(bb.position() + 5);
                for (int i = 0; i < n; i++) {
                    fp.filters.add(parseV1Filter(bb));
                }
                break;
            case 2:
                n = bb.get() & 0xFF;
                // Skip reserved bytes
                bb.position(bb.position() + 6);
                for (int i = 0; i < n; i++) {
                    fp.filters.add(parseV2Filter(bb));
                }
                break;
            default:
                throw new IOException("Unsupported filter pipeline version");
        }
        return fp;
    }

    /**
     * Parses a single filter of version 1 from the ByteBuffer.
     *
     * @param bb The ByteBuffer to read from.
     * @return The parsed Filter object.
     */
    private static Filter parseV1Filter(ByteBuffer bb) {
        Filter f = new Filter();
        f.id = bb.getShort() & 0xFFFF;
        short nameLen = bb.getShort();
        f.flags = bb.getShort() & 0xFFFF;
        short c = bb.getShort();

        if (nameLen > 0) {
            byte[] nameBytes = new byte[nameLen];
            bb.get(nameBytes);
            f.name = new String(nameBytes);
            // Skip padding bytes to align to a 4-byte boundary
            int pad = nameLen % 4;
            if (pad > 0) {
                bb.position(bb.position() + (4 - pad));
            }
        }

        f.clientData = new int[c];
        for (int j = 0; j < c; j++) {
            f.clientData[j] = bb.getInt();
        }

        // Skip padding for client data
        if (c % 2 != 0) {
            bb.getInt();
        }
        return f;
    }

    /**
     * Parses a single filter of version 2 from the ByteBuffer.
     *
     * @param bb The ByteBuffer to read from.
     * @return The parsed Filter object.
     */
    private static Filter parseV2Filter(ByteBuffer bb) {
        Filter f = new Filter();
        f.id = bb.getShort() & 0xFFFF;
        short c = bb.getShort();
        f.clientData = new int[c];
        for (int j = 0; j < c; j++) {
            f.clientData[j] = bb.getInt();
        }
        return f;
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

    //<editor-fold desc="Refactored readIndirectBlock Helper Methods">
    private static int calculateNrows(FractalHeapHeader header, long iblockSize, int passedNrows) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        if (iblockSize <= 0) {
            return passedNrows;
        }

        long startingBlockSize = header.startingBlockSize.getInstance(Long.class);
        long covered = 0;
        long current = startingBlockSize;
        int nr = 0;
        while (covered < iblockSize) {
            covered += (long) header.tableWidth * current;
            current *= 2;
            nr++;
        }
        return nr;
    }

    private static int calculateChildrenEntriesSize(FractalHeapHeader header, int nrows, FixedPointDatatype sizeOfOffset, FixedPointDatatype sizeOfLength) {
        int entriesSize = 0;
        for (short r = 0; r < nrows; r++) {
            for (int c = 0; c < header.tableWidth; c++) {
                entriesSize += sizeOfOffset.getSize();
                boolean isDirect = r < header.maxDblockRows;
                if (header.hasFilters && isDirect) {
                    entriesSize += sizeOfLength.getSize();
                    entriesSize += 4; // filter mask
                }
            }
        }
        return entriesSize;
    }

    private static void readAndVerifyIndirectBlockHeader(ByteBuffer blockBuffer, long expectedBlockOffset, FixedPointDatatype sizeOfOffset, FixedPointDatatype offsetBytes) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        byte[] signatureBytes = new byte[4];
        blockBuffer.get(signatureBytes);
        String sig = new String(signatureBytes, StandardCharsets.US_ASCII);
        if (!"FHIB".equals(sig)) {
            throw new IOException("Invalid indirect block signature: " + sig);
        }

        byte version = blockBuffer.get();
        if (version != 0) {
            throw new IOException("Unsupported version: " + version);
        }

        HdfFixedPoint heapHeader = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, blockBuffer);
        if (heapHeader.isUndefined()) {
            throw new IOException("Invalid heap header address in indirect block");
        }

        HdfFixedPoint blockOffset = HdfReadUtils.readHdfFixedPointFromBuffer(offsetBytes, blockBuffer);
        if (blockOffset.getInstance(Long.class) != expectedBlockOffset) {
            throw new IOException("Block offset mismatch");
        }
    }

    private static List<ChildInfo> parseChildInfos(ByteBuffer blockBuffer, FractalHeapHeader header, int nrows, long startOffset, FixedPointDatatype sizeOfOffset, FixedPointDatatype sizeOfLength) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        List<ChildInfo> childInfos = new ArrayList<>();
        long currentOffset = startOffset;
        long startingBlockSize = header.startingBlockSize.getInstance(Long.class);

        for (short r = 0; r < nrows; r++) {
            long rowBlockSize = startingBlockSize * (1L << r);
            for (int c = 0; c < header.tableWidth; c++) {
                HdfFixedPoint childAddress = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfOffset, blockBuffer);
                HdfFixedPoint childFilteredSize = sizeOfOffset.undefined();
                long childFilterMask = 0;

                boolean isDirect = r < header.maxDblockRows;
                if (header.hasFilters && isDirect) {
                    childFilteredSize = HdfReadUtils.readHdfFixedPointFromBuffer(sizeOfLength, blockBuffer);
                    childFilterMask = Integer.toUnsignedLong(blockBuffer.getInt());
                }

                if (!childAddress.isUndefined()) {
                    childInfos.add(new ChildInfo(childAddress, currentOffset, isDirect, childFilteredSize, childFilterMask, rowBlockSize));
                }
                currentOffset += rowBlockSize;
            }
        }
        return childInfos;
    }

    private static List<Block> readChildren(SeekableByteChannel channel, FractalHeapHeader header, List<ChildInfo> childInfos, FixedPointDatatype sizeOfOffset, FixedPointDatatype sizeOfLength) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        List<Block> children = new ArrayList<>();
        for (ChildInfo info : childInfos) {
            Block child;
            if (info.isDirect) {
                child = readDirectBlock(channel, header, info.address, sizeOfOffset, info.blockOffset, info.filteredSize, info.filterMask);
            } else {
                child = readIndirectBlock(channel, header, sizeOfOffset, sizeOfLength, info.address, info.blockOffset, info.blockSize, 0);
            }
            children.add(child);
        }
        return children;
    }
    //</editor-fold>

    private static Block readIndirectBlock(SeekableByteChannel channel, FractalHeapHeader header, FixedPointDatatype sizeOfOffset, FixedPointDatatype sizeOfLength, HdfFixedPoint address, long expectedBlockOffset, long iblockSize, int passedNrows) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        if (address.isUndefined()) {
            throw new IOException("Invalid indirect block address");
        }
        channel.position(address.getInstance(Long.class));

        // 1. Determine the number of rows in this indirect block.
        int nrows = calculateNrows(header, iblockSize, passedNrows);

        // 2. Calculate the total size of the child entries section.
        int childrenEntriesSize = calculateChildrenEntriesSize(header, nrows, sizeOfOffset, sizeOfLength);

        // 3. Read the entire block's header, entries, and checksum into a buffer.
        int headerPortionSize = 4 + 1 + sizeOfOffset.getSize() + header.offsetBytes.getSize(); // sig+ver+heapAddr+blockOffset
        int totalSizeToRead = headerPortionSize + childrenEntriesSize + 4; // 4 for checksum
        ByteBuffer blockBuffer = ByteBuffer.allocate(totalSizeToRead).order(ByteOrder.LITTLE_ENDIAN);
        int bytesRead = channel.read(blockBuffer);
        if (totalSizeToRead != bytesRead) {
            throw new IllegalStateException("Incorrect bytesRead: " + bytesRead + ": expected " + totalSizeToRead);
        }
        blockBuffer.flip();

        // 4. Verify the block's header fields. The buffer position is advanced past the header.
        readAndVerifyIndirectBlockHeader(blockBuffer, expectedBlockOffset, sizeOfOffset, header.offsetBytes);

        // 5. Parse child information from the buffer. The buffer position is advanced past the entries.
        List<ChildInfo> childInfos = parseChildInfos(blockBuffer, header, nrows, expectedBlockOffset, sizeOfOffset, sizeOfLength);

        // 6. Create the block object and read the checksum from the end of the buffer.
        IndirectBlock ib = new IndirectBlock();
        ib.nrows = nrows;
        ib.blockOffset = expectedBlockOffset;
        ib.checksum = Integer.toUnsignedLong(blockBuffer.getInt());

        // 7. Recursively read all children based on the parsed info.
        ib.children = readChildren(channel, header, childInfos, sizeOfOffset, sizeOfLength);

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
}