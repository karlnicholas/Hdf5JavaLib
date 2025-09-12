package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.StringJoiner;

/**
 * Represents the Fractal Heap Header.
 * This class models all fields as defined in the HDF5 specification for Version 0 headers.
 */
public class FractalHeapHeader {

    // --- Fields as defined in the HDF5 Specification ---
    public String signature;
    public byte version;
    public short heapIdLength;
    public short ioFilterEncodedLength;
    public byte flags;
    public int maxSizeOfManagedObjects;
    public long nextHugeObjectId;
    public long btreeAddressOfHugeObjects;
    public long amountOfFreeSpace;
    public long addressOfFreeSpaceManager;
    public long amountOfManagedSpace;
    public long allocatedManagedSpace;
    public long directBlockAllocationIterator;
    public long numberOfManagedObjects;
    public long sizeOfHugeObjectsInHeap;
    public long numberOfHugeObjectsInHeap;
    public long sizeOfTinyObjectsInHeap;
    public long numberOfTinyObjectsInHeap;
    public short tableWidth;
    public long startingBlockSize;
    public long maximumDirectBlockSize;
    public short maximumHeapSize; // This is log2 of the max size
    public short startingRowsInRoot;
    public long rootBlockAddress;
    public short currentRowsInRoot;
    public byte[] ioFilterPipeline;
    public int checksum;

    // --- Derived/Helper fields ---
    public final int sizeOfOffsets;
    public final int sizeOfLengths;
    public int maxRows;

    private FractalHeapHeader(int sizeOfOffsets, int sizeOfLengths) {
        this.sizeOfOffsets = sizeOfOffsets;
        this.sizeOfLengths = sizeOfLengths;
    }

    public static FractalHeapHeader read(SeekableByteChannel channel, long position, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        FractalHeapHeader header = new FractalHeapHeader(sizeOfOffsets, sizeOfLengths);

        channel.position(position);
        ByteBuffer preliminaryBuffer = ByteBuffer.allocate(9);
        if (channel.read(preliminaryBuffer) != 9) throw new IOException("Could not read preliminary header data.");
        preliminaryBuffer.flip();
        DataReader prelimReader = new DataReader(preliminaryBuffer);
        prelimReader.position(7);
        header.ioFilterEncodedLength = prelimReader.readShort();

        int headerSize = 4 + 1 + 2 + 2 + 1 + 4 +
                sizeOfLengths + sizeOfOffsets + sizeOfLengths + sizeOfOffsets +
                sizeOfLengths + sizeOfLengths + sizeOfLengths + sizeOfLengths +
                sizeOfLengths + sizeOfLengths + sizeOfLengths + sizeOfLengths +
                2 + sizeOfLengths + sizeOfLengths +
                2 + // Maximum Heap Size (log2) is 2 bytes.
                2 + sizeOfOffsets + 2 +
                header.ioFilterEncodedLength + 4;

        channel.position(position);
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize);
        if (channel.read(headerBuffer) != headerSize) throw new IOException("Could not read the full Fractal Heap Header of size " + headerSize);
        headerBuffer.flip();
        DataReader reader = new DataReader(headerBuffer);

        header.signature = reader.readString(4);
        if (!"FRHP".equals(header.signature)) throw new IOException("Invalid Fractal Heap signature: " + header.signature);
        header.version = reader.readByte();
        header.heapIdLength = reader.readShort();
        reader.readShort(); // Skip ioFilterEncodedLength
        header.flags = reader.readByte();
        header.maxSizeOfManagedObjects = reader.readInt();
        header.nextHugeObjectId = reader.readSizedValue(sizeOfLengths);
        header.btreeAddressOfHugeObjects = reader.readSizedValue(sizeOfOffsets);
        header.amountOfFreeSpace = reader.readSizedValue(sizeOfLengths);
        header.addressOfFreeSpaceManager = reader.readSizedValue(sizeOfOffsets);
        header.amountOfManagedSpace = reader.readSizedValue(sizeOfLengths);
        header.allocatedManagedSpace = reader.readSizedValue(sizeOfLengths);
        header.directBlockAllocationIterator = reader.readSizedValue(sizeOfLengths);
        header.numberOfManagedObjects = reader.readSizedValue(sizeOfLengths);
        header.sizeOfHugeObjectsInHeap = reader.readSizedValue(sizeOfLengths);
        header.numberOfHugeObjectsInHeap = reader.readSizedValue(sizeOfLengths);
        header.sizeOfTinyObjectsInHeap = reader.readSizedValue(sizeOfLengths);
        header.numberOfTinyObjectsInHeap = reader.readSizedValue(sizeOfLengths);
        header.tableWidth = reader.readShort();
        header.startingBlockSize = reader.readSizedValue(sizeOfLengths);
        header.maximumDirectBlockSize = reader.readSizedValue(sizeOfLengths);
        header.maximumHeapSize = reader.readShort();
        header.startingRowsInRoot = reader.readShort();
        header.rootBlockAddress = reader.readSizedValue(sizeOfOffsets);
        header.currentRowsInRoot = reader.readShort();
        if (header.ioFilterEncodedLength > 0) {
            header.ioFilterPipeline = reader.readBytes(header.ioFilterEncodedLength);
        } else {
            header.ioFilterPipeline = new byte[0];
        }
        header.checksum = reader.readInt();

        if (header.startingBlockSize > 0) {
            int startingBlockSizePowerOf2 = (int) Math.round(Math.log(header.startingBlockSize) / Math.log(2));
            header.maxRows = header.maximumHeapSize - startingBlockSizePowerOf2;
        } else {
            header.maxRows = 0;
        }

        return header;
    }

    /**
     * Calculates the size of a block at a given depth in the fractal heap tree.
     * This method now correctly computes the size based on the block's type (Direct vs. Indirect)
     * and its contents.
     *
     * @param depth The depth of the block in the tree (0 for root).
     * @return The size of the block in bytes.
     */
    public long getBlockSize(int depth) {
        // The tree has depths 0, 1, ..., (currentRowsInRoot - 1) for indirect blocks.
        // Direct blocks are at depth `currentRowsInRoot`.
        if (depth >= this.currentRowsInRoot) {
            return this.maximumDirectBlockSize;
        }

        // It's an indirect block. Calculate its row number, M.
        // Rows are from the bottom up (0 = direct blocks' "row"). M = (currentRowsInRoot - 1) - d.
        int rowNumberM = (this.currentRowsInRoot - 1) - depth;
        if (rowNumberM < 0) {
            // This can happen for a direct root (currentRowsInRoot=0), which is handled by the first if.
            throw new IllegalStateException("Calculated negative row number for depth " + depth);
        }

        // Number of child entries in the indirect block at row M is k = starting_rows + 2 * M
        int numEntriesK = this.startingRowsInRoot + (2 * rowNumberM);

        // Calculate size: Header (sig:4, ver:1, heap_addr:sizeOfOffsets) + Table (k * sizeOfOffsets) + Checksum (4)
        return (4L + 1L + this.sizeOfOffsets) + ((long)numEntriesK * this.sizeOfOffsets) + 4L;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", FractalHeapHeader.class.getSimpleName() + "[", "]")
                .add("signature='" + signature + "'").add("version=" + version).add("flags=" + flags)
                .add("sizeOfOffsets=" + sizeOfOffsets).add("sizeOfLengths=" + sizeOfLengths)
                .add("tableWidth=" + tableWidth).add("startingBlockSize=" + startingBlockSize)
                .add("maximumDirectBlockSize=" + maximumDirectBlockSize).add("maximumHeapSize(log2)=" + maximumHeapSize)
                .add("maxRows=" + maxRows).add("numberOfManagedObjects=" + numberOfManagedObjects)
                .add("startingRowsInRoot=" + startingRowsInRoot).add("currentRowsInRoot=" + currentRowsInRoot)
                .add("rootBlockAddress=" + rootBlockAddress)
                .toString();
    }
}