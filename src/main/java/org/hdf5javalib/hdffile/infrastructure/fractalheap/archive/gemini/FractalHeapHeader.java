package org.hdf5javalib.hdffile.infrastructure.fractalheap.archive.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

class FractalHeapHeader {
    public final String signature;
    public final short version;
    public final int heapIdLength;
    public final int ioFiltersEncodedLength;
    public final short flags;
    public final long maximumSizeOfManagedObjects;
    public final long nextHugeObjectId;
    public final long v2BtreeAddressOfHugeObjects;
    public final long amountOfFreeSpaceInManagedBlocks;
    public final long addressOfManagedBlockFreeSpaceManager;
    public final long amountOfManagedSpaceInHeap;
    public final long amountOfAllocatedManagedSpaceInHeap;
    public final long offsetOfDirectBlockAllocationIterator;
    public final long numberOfManagedObjectsInHeap;
    public final long sizeOfHugeObjectsInHeap;
    public final long numberOfHugeObjectsInHeap;
    public final long sizeOfTinyObjectsInHeap;
    public final long numberOfTinyObjectsInHeap;
    public final int tableWidth;
    public final long startingBlockSize;
    public final long maximumDirectBlockSize;
    public final int maximumHeapSize;
    public final int startingRowsInRootIndirectBlock;
    public final long addressOfRootBlock;
    public final int currentRowsInRootIndirectBlock;
    public final Long sizeOfFilteredRootDirectBlock;
    public final Integer ioFilterMask;
    public final byte[] ioFilterInformation;
    public final int checksum;

    // Main constructor for reading
    private FractalHeapHeader(ByteBuffer bb, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        bb.order(ByteOrder.LITTLE_ENDIAN);

        this.signature = Hdf5Utils.readSignature(bb, "FRHP");
        this.version = bb.get();
        this.heapIdLength = bb.getShort() & 0xFFFF;
        this.ioFiltersEncodedLength = bb.getShort() & 0xFFFF;
        this.flags = bb.get();
        this.maximumSizeOfManagedObjects = bb.getInt() & 0xFFFFFFFFL;
        this.nextHugeObjectId = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.v2BtreeAddressOfHugeObjects = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        this.amountOfFreeSpaceInManagedBlocks = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.addressOfManagedBlockFreeSpaceManager = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        this.amountOfManagedSpaceInHeap = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.amountOfAllocatedManagedSpaceInHeap = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.offsetOfDirectBlockAllocationIterator = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.numberOfManagedObjectsInHeap = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.sizeOfHugeObjectsInHeap = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.numberOfHugeObjectsInHeap = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.sizeOfTinyObjectsInHeap = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.numberOfTinyObjectsInHeap = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.tableWidth = bb.getShort() & 0xFFFF;
        this.startingBlockSize = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.maximumDirectBlockSize = Hdf5Utils.readLength(bb, sizeOfLengths);
        this.maximumHeapSize = bb.getShort() & 0xFFFF;
        this.startingRowsInRootIndirectBlock = bb.getShort() & 0xFFFF;
        this.addressOfRootBlock = Hdf5Utils.readOffset(bb, sizeOfOffsets);
        this.currentRowsInRootIndirectBlock = bb.getShort() & 0xFFFF;

        if (this.ioFiltersEncodedLength > 0) {
            this.sizeOfFilteredRootDirectBlock = Hdf5Utils.readLength(bb, sizeOfLengths);
            this.ioFilterMask = bb.getInt();
            this.ioFilterInformation = new byte[this.ioFiltersEncodedLength];
            bb.get(this.ioFilterInformation);
        } else {
            this.sizeOfFilteredRootDirectBlock = null;
            this.ioFilterMask = null;
            this.ioFilterInformation = null;
        }
        this.checksum = Hdf5Utils.readChecksum(bb);
    }

    // Constructor for mocking
    FractalHeapHeader(String sig, short v, int hIdLen, int ioLen, short fl, long maxObj, long nxtHuge, long btree, long free, long freeMgr, long manSpace, long allocSpace, long iter, long nMan, long szHuge, long nHuge, long szTiny, long nTiny, int tW, long startBlk, long maxBlk, int maxHp, int startRow, long rootAddr, int currRow, Long filtSz, Integer filtMsk, byte[] filtInfo, int cs) {
        signature = sig; version = v; heapIdLength = hIdLen; ioFiltersEncodedLength = ioLen; flags = fl; maximumSizeOfManagedObjects = maxObj; nextHugeObjectId = nxtHuge; v2BtreeAddressOfHugeObjects = btree; amountOfFreeSpaceInManagedBlocks = free; addressOfManagedBlockFreeSpaceManager = freeMgr; amountOfManagedSpaceInHeap = manSpace; amountOfAllocatedManagedSpaceInHeap = allocSpace; offsetOfDirectBlockAllocationIterator = iter; numberOfManagedObjectsInHeap = nMan; sizeOfHugeObjectsInHeap = szHuge; numberOfHugeObjectsInHeap = nHuge; sizeOfTinyObjectsInHeap = szTiny; numberOfTinyObjectsInHeap = nTiny; tableWidth = tW; startingBlockSize = startBlk; maximumDirectBlockSize = maxBlk; maximumHeapSize = maxHp; startingRowsInRootIndirectBlock = startRow; addressOfRootBlock = rootAddr; currentRowsInRootIndirectBlock = currRow; sizeOfFilteredRootDirectBlock = filtSz; ioFilterMask = filtMsk; ioFilterInformation = filtInfo; checksum = cs;
    }

    public static FractalHeapHeader read(SeekableByteChannel channel, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        long originalPos = channel.position();
        channel.position(originalPos + 4 + 1 + 2); // sig + ver + heapIdLen
        ByteBuffer filterLenBuf = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
        channel.read(filterLenBuf);
        filterLenBuf.flip();
        int ioFilterLength = filterLenBuf.getShort() & 0xFFFF;
        channel.position(originalPos);

        int headerSize = 101 + (11 * sizeOfLengths) + (3 * sizeOfOffsets) - 48; // Base size for 8/8 offsets/lengths
        if (ioFilterLength > 0) {
            headerSize += sizeOfLengths + 4 + ioFilterLength;
        }

        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize);
        Hdf5Utils.readBytes(channel, headerBuffer);
        return new FractalHeapHeader(headerBuffer, sizeOfOffsets, sizeOfLengths);
    }

//    public byte[] toByteArray() {
//        // Simplified for mocking. A real implementation would be more robust.
//        ByteBuffer bb = ByteBuffer.allocate(256).order(ByteOrder.LITTLE_ENDIAN);
//        bb.put(signature.getBytes()).put(version).putShort((short)heapIdLength).putShort((short)ioFiltersEncodedLength).put(flags).putInt((int)maximumSizeOfManagedObjects);
//        bb.putLong(nextHugeObjectId).putLong(v2BtreeAddressOfHugeObjects).putLong(amountOfFreeSpaceInManagedBlocks).putLong(addressOfManagedBlockFreeSpaceManager).putLong(amountOfManagedSpaceInHeap).putLong(amountOfAllocatedManagedSpaceInHeap).putLong(offsetOfDirectBlockAllocationIterator).putLong(numberOfManagedObjectsInHeap).putLong(sizeOfHugeObjectsInHeap).putLong(numberOfHugeObjectsInHeap).putLong(sizeOfTinyObjectsInHeap).putLong(numberOfTinyObjectsInHeap);
//        bb.putShort((short)tableWidth).putLong(startingBlockSize).putLong(maximumDirectBlockSize).putShort((short)maximumHeapSize).putShort((short)startingRowsInRootIndirectBlock).putLong(addressOfRootBlock).putShort((short)currentRowsInRootIndirectBlock);
//        if (ioFiltersEncodedLength > 0) { /* Omitted for mock */ }
//        bb.putInt(checksum);
//        byte[] result = new byte[bb.position()];
//        bb.flip();
//        bb.get(result);
//        return result;
//    }
}
