package org.hdf5javalib.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * An in-memory implementation of {@link SeekableByteChannel}.
 * <p>
 * The {@code MemorySeekableByteChannel} class provides a seekable byte channel that
 * stores data in a fixed-size {@link ByteBuffer}. It supports reading, writing,
 * positioning, truncating, and closing operations, making it suitable for testing
 * or scenarios where in-memory data manipulation is needed, such as simulating
 * file I/O for HDF5 data processing. The buffer is allocated with a fixed capacity
 * that is assumed to be sufficient for all operations.
 * </p>
 */
public class MemorySeekableByteChannel implements SeekableByteChannel {
    /** The internal ByteBuffer storing the channel's data. */
    private ByteBuffer buffer;
    /** Indicates whether the channel is open. */
    private boolean open;

    /**
     * Constructs a MemorySeekableByteChannel with the specified fixed capacity.
     *
     * @param initialCapacity the fixed capacity of the internal ByteBuffer
     */
    public MemorySeekableByteChannel(int initialCapacity) {
        this.buffer = ByteBuffer.allocate(initialCapacity);
        this.open = true;
    }

    /**
     * Checks if the channel is open.
     *
     * @return true if the channel is open, false otherwise
     */
    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Closes the channel.
     *
     * @throws IOException if the channel is already closed
     */
    @Override
    public void close() throws IOException {
        open = false;
    }

    /**
     * Reads bytes from the channel into the destination ByteBuffer.
     *
     * @param dst the destination ByteBuffer to read into
     * @return the number of bytes read, or -1 if the end of the buffer is reached
     * @throws IOException if the channel is closed
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        if (buffer.remaining() == 0) return -1;

        int bytesToRead = Math.min(dst.remaining(), buffer.remaining());
        byte[] data = new byte[bytesToRead];
        buffer.get(data);
        dst.put(data);
        return bytesToRead;
    }

    /**
     * Writes bytes from the source ByteBuffer to the channel.
     *
     * @param src the source ByteBuffer to write from
     * @return the number of bytes written
     * @throws IOException if the channel is closed or if the write exceeds the buffer's capacity
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        if (src.remaining() > buffer.remaining()) {
            throw new IOException("Write exceeds buffer capacity");
        }

        int bytesToWrite = src.remaining();
        buffer.put(src);
        return bytesToWrite;
    }

    /**
     * Returns the current position in the channel.
     *
     * @return the current position
     * @throws IOException if the channel is closed
     */
    @Override
    public long position() throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        return buffer.position();
    }

    /**
     * Sets the position in the channel.
     *
     * @param newPosition the new position
     * @return this channel
     * @throws IOException if the channel is closed
     * @throws IllegalArgumentException if the position is negative or exceeds the buffer capacity
     */
    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        if (newPosition < 0 || newPosition > buffer.capacity()) {
            throw new IllegalArgumentException("Position out of bounds: " + newPosition);
        }
        buffer.position((int) newPosition);
        return this;
    }

    /**
     * Returns the size of the channel, which is the buffer's fixed capacity.
     *
     * @return the fixed capacity of the buffer
     * @throws IOException if the channel is closed
     */
    @Override
    public long size() throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        return buffer.capacity();
    }

    /**
     * Truncates the channel to the specified size.
     *
     * @param size the new size
     * @return this channel
     * @throws IOException if the channel is closed or if the size exceeds the buffer's capacity
     * @throws IllegalArgumentException if the size is negative
     */
    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        if (size < 0) throw new IllegalArgumentException("Size cannot be negative");
        if (size > buffer.capacity()) {
            throw new IOException("Truncate size exceeds buffer capacity: " + size);
        }
        if (size < buffer.position()) {
            buffer.position((int) size);
        }
        return this;
    }

    /**
     * Returns a copy of the channel's data as a byte array.
     * <p>
     * The buffer's position is rewound to the beginning, and all data up to the
     * buffer's capacity is copied into a new byte array. The buffer's position is
     * then restored.
     * </p>
     *
     * @return a byte array containing the channel's data
     */
    public byte[] toByteArray() {
        int originalPosition = buffer.position();
        buffer.rewind();
        byte[] data = new byte[buffer.capacity()];
        buffer.get(data);
        buffer.position(originalPosition);
        return data;
    }
}