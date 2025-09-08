package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//##############################################################################
//### FRACTAL HEAP STRUCTURES
//##############################################################################
public class FractalHeap {
    public final FractalHeapHeader header;

    // The rootBlock object is removed. This class is now a "handle".
    // private final Object rootBlock; // REMOVED

    // Context needed for object retrieval
    private final SeekableByteChannel channel;
    private final int sizeOfOffsets;
    private final int sizeOfLengths;
    private final int maxDirectRows;

    // Cache to store blocks that have already been read from disk.
    // Key: Block Address, Value: Block Object (Direct or Indirect)
    private final Map<Long, Object> blockCache;

    /**
     * Helper class to return both address and size of a located direct block.
     */
    private static class DirectBlockInfo {
        final long address;
        final long size;

        DirectBlockInfo(long address, long size) {
            this.address = address;
            this.size = size;
        }
    }

    // MODIFIED CONSTRUCTOR: No longer accepts a rootBlock object.
    public FractalHeap(FractalHeapHeader header, SeekableByteChannel channel, int sizeOfOffsets, int sizeOfLengths) {
        this.header = header;
        this.channel = channel;
        this.sizeOfOffsets = sizeOfOffsets;
        this.sizeOfLengths = sizeOfLengths;
        this.blockCache = new HashMap<>();

        // Pre-calculate max rows for direct blocks in any indirect block
        int log2MaxDirect = Hdf5Utils.ceilLog2(header.maximumDirectBlockSize);
        int log2Start = Hdf5Utils.ceilLog2(header.startingBlockSize);
        this.maxDirectRows = (log2MaxDirect - log2Start) + 2;
    }

    public byte[] getObject(byte[] rawId) throws IOException {
        if (header.addressOfRootBlock == Hdf5Utils.UNDEFINED_ADDRESS) {
            throw new IOException("Cannot get object; fractal heap has no root block.");
        }

        ParsedHeapId id = new ParsedHeapId(rawId, this.header);


        switch (id.type) {
            case 0: // Managed Object
                return findManagedObject(id);
            case 1: // Huge Object
                throw new UnsupportedOperationException("Huge object retrieval not implemented.");
            case 2: // Tiny Object
                int headerBits = 8; // type(2), version(2), reserved(4)
                long minOfMaxs = Math.min(header.maximumDirectBlockSize, header.maximumSizeOfManagedObjects);
                if (minOfMaxs < (1 << 8)) {
                    headerBits += 8;
                } else if (minOfMaxs < (1 << 16)) {
                    headerBits += 16;
                } else if (minOfMaxs < (1 << 24)) {
                    headerBits += 24;
                } else {
                    headerBits += 32;
                }
                int dataOffsetInIdBytes = (headerBits + 7) / 8;
                return Arrays.copyOfRange(rawId, dataOffsetInIdBytes, dataOffsetInIdBytes + id.length);
            default:
                throw new IOException("Unknown heap object type: " + id.type);
        }
    }

    private byte[] findManagedObject(ParsedHeapId id) throws IOException {
        // 1. Find the address and size of the direct block containing our object's offset.
        DirectBlockInfo blockInfo = findDirectBlockForOffset(id.offset);
        if (blockInfo.address == Hdf5Utils.UNDEFINED_ADDRESS) {
            throw new IOException("Could not locate direct block for heap offset: " + id.offset);
        }

        // 2. Read that specific direct block USING THE CACHED HELPER.
        FractalHeapDirectBlock directBlock = getDirectBlock(blockInfo.address, blockInfo.size);

        // 3. The heap ID offset is from the start of the heap. The block's offset is also from the start of the heap.
        //    We need the offset *within* this block's data array.
        long offsetInBlock = id.offset - directBlock.blockOffset;

        // 4. Sanity checks.
        if (offsetInBlock < 0 || offsetInBlock + id.length > directBlock.objectData.length) {
            throw new IOException("Heap ID points outside of the located direct block's bounds. HeapID: " + id +
                    ", Block Offset: " + directBlock.blockOffset + ", Calculated Offset in Block: " + offsetInBlock);
        }

        // 5. Extract and return the object's data.
        return Arrays.copyOfRange(directBlock.objectData, (int) offsetInBlock, (int) (offsetInBlock + id.length));
    }

    private DirectBlockInfo findDirectBlockForOffset(long targetOffset) throws IOException {
        if (header.currentRowsInRootIndirectBlock == 0) {
            // Case 1: The root is a direct block.
            if (targetOffset < header.startingBlockSize) {
                return new DirectBlockInfo(header.addressOfRootBlock, header.startingBlockSize);
            } else {
                throw new IOException("Target offset " + targetOffset + " is out of bounds for a direct root block of size " + header.startingBlockSize);
            }
        } else {
            // Case 2: The root is an indirect block.
            return findAddressInIndirectBlock(header.addressOfRootBlock, header.currentRowsInRootIndirectBlock, targetOffset);
        }
    }

    private DirectBlockInfo findAddressInIndirectBlock(long indirectBlockAddr, int rows, long targetOffset) throws IOException {
        // Read the indirect block USING THE CACHED HELPER.
        FractalHeapIndirectBlock indirectBlock = getIndirectBlock(indirectBlockAddr, rows);

        for (int i = 0; i < indirectBlock.childDirectBlockAddresses.length; i++) {
            long childStartOffset = indirectBlock.blockOffsets[i];

            int row = i / header.tableWidth;
            long childBlockSize = header.startingBlockSize * (1L << row);
            long childEndOffset = childStartOffset + childBlockSize;

            if (targetOffset >= childStartOffset && targetOffset < childEndOffset) {
                long childAddress = indirectBlock.childDirectBlockAddresses[i];
                if (childAddress == Hdf5Utils.UNDEFINED_ADDRESS) {
                    throw new IOException("Found entry for offset " + targetOffset + " but the block address is undefined.");
                }
                return new DirectBlockInfo(childAddress, childBlockSize);
            }
        }

        if (indirectBlock.childIndirectBlockAddresses.length > 0) {
            throw new UnsupportedOperationException("Searching for objects in nested indirect blocks is not yet implemented.");
        }

        throw new IOException("Failed to find a direct block containing offset " + targetOffset);
    }

    // --- Caching Helper Methods ---

    private FractalHeapDirectBlock getDirectBlock(long address, long size) throws IOException {
        return (FractalHeapDirectBlock) blockCache.computeIfAbsent(address, addr -> {
            try {
                return FractalHeapDirectBlock.read(channel, (Long) addr, size, header, sizeOfOffsets);
            } catch (IOException e) {
                // Lambda expressions can't throw checked exceptions directly, so we wrap them.
                throw new RuntimeException(e);
            }
        });
    }

    private FractalHeapIndirectBlock getIndirectBlock(long address, int rows) throws IOException {
        try {
            return (FractalHeapIndirectBlock) blockCache.computeIfAbsent(address, addr -> {
                try {
                    return FractalHeapIndirectBlock.read(channel, (Long) addr, rows, header, sizeOfOffsets, sizeOfLengths);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch(RuntimeException e) {
            // Unwrap the original IOException
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw e;
        }
    }
}