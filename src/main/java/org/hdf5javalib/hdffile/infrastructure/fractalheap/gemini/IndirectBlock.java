package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class IndirectBlock implements Block {
    public final String signature;
    public final byte version;
    public final long heapHeaderAddress;
    public final long blockOffset;
    public final List<Block> childBlocks;
    public final int checksum;

    private IndirectBlock(String signature, byte version, long heapHeaderAddress, long blockOffset, List<Block> childBlocks, int checksum) {
        this.signature = signature;
        this.version = version;
        this.heapHeaderAddress = heapHeaderAddress;
        this.blockOffset = blockOffset;
        this.childBlocks = childBlocks;
        this.checksum = checksum;
    }

    @Override
    public long getBlockOffset() { return blockOffset; }

    public static IndirectBlock read(SeekableByteChannel channel, long position, FractalHeapHeader header, int depth) throws IOException {
        long blockSize = header.getBlockSize(depth);
        if (blockSize > Integer.MAX_VALUE || blockSize <= 0) {
            throw new IOException("Invalid indirect block size: " + blockSize);
        }

        channel.position(position);
        ByteBuffer buffer = ByteBuffer.allocate((int) blockSize);
        int bytesRead = channel.read(buffer);
        if(bytesRead < blockSize) {
            throw new IOException("Could not read full indirect block. Expected " + blockSize + " but got " + bytesRead);
        }
        buffer.flip();
        DataReader reader = new DataReader(buffer);

        String signature = reader.readString(4);
        if (!"FHIB".equals(signature)) throw new IOException("Invalid Indirect Block signature: " + signature);

        byte version = reader.readByte();
        long heapHeaderAddress = reader.readSizedValue(header.sizeOfOffsets);
        long blockOffset = position;

        int rowNumberM = (header.currentRowsInRoot - 1) - depth;
        int numEntries = header.startingRowsInRoot + (2 * rowNumberM);

        List<Block> childBlocks = new ArrayList<>();
        int childDepth = depth + 1;
        boolean isChildDirect = (childDepth >= header.currentRowsInRoot);

        long undefinedAddr = (header.sizeOfOffsets == 8) ? -1L : (1L << (header.sizeOfOffsets * 8)) - 1;

        for (int i = 0; i < numEntries; i++) {
            long childAddress = reader.readSizedValue(header.sizeOfOffsets);
            if (childAddress != undefinedAddr) {
                if (isChildDirect) {
                    childBlocks.add(DirectBlock.read(channel, childAddress, header, childDepth));
                } else {
                    childBlocks.add(IndirectBlock.read(channel, childAddress, header, childDepth));
                }
            } else {
                childBlocks.add(null);
            }
        }

        int checksum = reader.readInt();

        return new IndirectBlock(signature, version, heapHeaderAddress, blockOffset, childBlocks, checksum);
    }

    @Override
    public String toString() {
        String childrenSummary = childBlocks.stream()
                .map(b -> b == null ? "null" : b.getClass().getSimpleName() + "@" + b.getBlockOffset())
                .collect(Collectors.joining(", "));

        return new StringJoiner(", ", IndirectBlock.class.getSimpleName() + "[", "]")
                .add("signature='" + signature + "'")
                .add("version=" + version)
                .add("blockOffset=" + blockOffset)
                .add("childCount=" + childBlocks.size())
                .add("children=[" + childrenSummary + "]")
                .toString();
    }
}