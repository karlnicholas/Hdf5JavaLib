package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.StringJoiner;

public class DirectBlock implements Block {
    public final String signature;
    public final byte version;
    public final long heapHeaderAddress;
    public final long blockOffset;
    public final byte[] data;
    public final int checksum;

    private DirectBlock(String signature, byte version, long heapHeaderAddress, long blockOffset, byte[] data, int checksum) {
        this.signature = signature;
        this.version = version;
        this.heapHeaderAddress = heapHeaderAddress;
        this.blockOffset = blockOffset;
        this.data = data;
        this.checksum = checksum;
    }

    @Override
    public long getBlockOffset() {
        return blockOffset;
    }

    /**
     * Reads a Direct Block from the channel.
     * @param channel The file channel.
     * @param position The absolute address of this block in the file.
     * @param header The already-parsed Fractal Heap Header.
     * @param depth The depth of this direct block in the tree structure.
     * @return A populated DirectBlock instance.
     */
    public static DirectBlock read(SeekableByteChannel channel, long position, FractalHeapHeader header, int depth) throws IOException {
        long blockSize = header.getBlockSize(depth);
        if (blockSize > Integer.MAX_VALUE || blockSize <= 0) {
            throw new IOException("Invalid direct block size: " + blockSize);
        }

        channel.position(position);
        ByteBuffer buffer = ByteBuffer.allocate((int) blockSize);
        int bytesRead = channel.read(buffer);
        if (bytesRead < blockSize) {
            throw new IOException("Could not read full direct block. Expected " + blockSize + " but got " + bytesRead);
        }
        buffer.flip();
        DataReader reader = new DataReader(buffer);

        String signature = reader.readString(4);
        if (!"FHDB".equals(signature)) throw new IOException("Invalid Direct Block signature: " + signature);

        byte version = reader.readByte();
        long heapHeaderAddress = reader.readSizedValue(header.sizeOfOffsets);
        long blockOffset = position;

        int headerSize = 4 + 1 + header.sizeOfOffsets;
        int checksumSize = 4;
        int dataSize = (int)blockSize - headerSize - checksumSize;

        byte[] data = reader.readBytes(dataSize);
        int checksum = reader.readInt();

        return new DirectBlock(signature, version, heapHeaderAddress, blockOffset, data, checksum);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DirectBlock.class.getSimpleName() + "[", "]")
                .add("signature='" + signature + "'")
                .add("version=" + version)
                .add("blockOffset=" + blockOffset)
                .add("data.length=" + data.length)
                .toString();
    }
}