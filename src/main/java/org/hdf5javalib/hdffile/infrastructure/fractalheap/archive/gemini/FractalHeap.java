package org.hdf5javalib.hdffile.infrastructure.fractalheap.archive.gemini;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

// package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;
// ... (imports)

public class FractalHeap {
    // ... (fields are mostly the same)
    public final FractalHeapHeader header;
    private final SeekableByteChannel channel;
    private final int sizeOfOffsets;
    private final int sizeOfLengths;
    private final Map<Long, Object> blockCache;

    // ### REASONING FOR CHANGE ###
    // The DirectBlockInfo needs to return not just the address and size of the located
    // block, but also its starting offset within the heap's linear address space. This
    // is essential for calculating the object's relative position inside the block.
    private static class DirectBlockInfo {
        final long address;       // File address of the block
        final long size;          // Size of the block
        final long startOffset;   // The block's starting offset in the heap's linear address space

        DirectBlockInfo(long address, long size, long startOffset) {
            this.address = address;
            this.size = size;
            this.startOffset = startOffset;
        }
    }

    public FractalHeap(FractalHeapHeader header, SeekableByteChannel channel, int sizeOfOffsets, int sizeOfLengths) {
        this.header = header;
        this.channel = channel;
        this.sizeOfOffsets = sizeOfOffsets;
        this.sizeOfLengths = sizeOfLengths;
        this.blockCache = new HashMap<>();
    }

    // ... (getObject method is fine)
    public byte[] getObject(byte[] rawId) throws IOException {
        // ... (existing implementation is good)
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
                // ... (existing tiny object logic seems plausible)
                return null;
            default:
                throw new IOException("Unknown heap object type: " + id.type);
        }
    }


    private byte[] findManagedObject(ParsedHeapId id) throws IOException {
        DirectBlockInfo blockInfo = findDirectBlockForOffset(id.offset);
        if (blockInfo.address == Hdf5Utils.UNDEFINED_ADDRESS) {
            throw new IOException("Could not locate direct block for heap offset: " + id.offset);
        }

        FractalHeapDirectBlock directBlock = getDirectBlock(blockInfo.address, blockInfo.size);

        // ### REASONING FOR CHANGE ###
        // The object's offset is absolute (from the start of the heap). The block's
        // offset is also absolute. To find the object's position *inside* the block's
        // data array, we must subtract the block's starting offset.
        // OLD: long offsetInBlock = id.offset;
        long offsetInBlock = id.offset - blockInfo.startOffset;

        if (offsetInBlock < 0 || offsetInBlock + id.length > directBlock.objectData.length) {
            throw new IOException("Heap ID points outside of the located direct block's bounds. HeapID offset: " + id.offset +
                    ", Block Start Offset: " + blockInfo.startOffset + ", Calculated Offset in Block: " + offsetInBlock +
                    ", Block Data Length: " + directBlock.objectData.length);
        }

        return Arrays.copyOfRange(directBlock.objectData, (int) offsetInBlock, (int) (offsetInBlock + id.length));
    }

    private DirectBlockInfo findDirectBlockForOffset(long targetOffset) throws IOException {
        if (header.currentRowsInRootIndirectBlock == 0) {
            if (targetOffset < header.startingBlockSize) {
                // The root is a direct block. Its starting offset in the heap is 0.
                return new DirectBlockInfo(header.addressOfRootBlock, header.startingBlockSize, 0);
            } else {
                throw new IOException("Target offset is out of bounds for a direct root block.");
            }
        } else {
            // The root is an indirect block.
            return findAddressInIndirectBlock(header.addressOfRootBlock, header.currentRowsInRootIndirectBlock, targetOffset);
        }
    }

    private DirectBlockInfo findAddressInIndirectBlock(long indirectBlockAddr, int rows, long targetOffset) throws IOException {
        FractalHeapIndirectBlock indirectBlock = getIndirectBlock(indirectBlockAddr, rows);

        // ### REASONING FOR CHANGE ###
        // This is the core logic fix. We start with the single known offset for the
        // indirect block, which covers its first child. Then, we loop and cumulatively
        // add the size of each child block to find the starting offset of the next one.
        long currentBlockStartOffset = indirectBlock.blockOffset;

        for (int i = 0; i < indirectBlock.childDirectBlockAddresses.length; i++) {
            // The row determines the size of the block this entry points to.
            int row = i / header.tableWidth;
            long childBlockSize = header.startingBlockSize * (1L << row);

            // Check if our target offset falls within the range of THIS child block.
            if (targetOffset >= currentBlockStartOffset && targetOffset < (currentBlockStartOffset + childBlockSize)) {
                long childAddress = indirectBlock.childDirectBlockAddresses[i];
                if (childAddress == Hdf5Utils.UNDEFINED_ADDRESS) {
                    // This space in the heap is allocated but the block hasn't been written to disk.
                    throw new IOException("Found entry for offset " + targetOffset + " but the block address is undefined.");
                }
                // We found it! Return the address, size, and calculated starting offset.
                return new DirectBlockInfo(childAddress, childBlockSize, currentBlockStartOffset);
            }

            // If not, add this block's size to the running offset and check the next entry.
            currentBlockStartOffset += childBlockSize;
        }

        if (indirectBlock.childIndirectBlockAddresses.length > 0) {
            // TODO: The same cumulative logic needs to be applied to find which child
            // indirect block to descend into. The `currentBlockStartOffset` would continue
            // to accumulate across the direct block section first.
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