package org.hdf5javalib.examples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class MemorySeekableByteChannel implements SeekableByteChannel {
    private ByteBuffer buffer;
    private boolean open;

    public MemorySeekableByteChannel(int initialCapacity) {
        this.buffer = ByteBuffer.allocate(initialCapacity);
        this.open = true;
    }

    public MemorySeekableByteChannel() {
        this(1024);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void close() throws IOException {
        open = false;
    }

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

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");

        int bytesToWrite = src.remaining();
        ensureCapacity(buffer.position() + bytesToWrite);

        buffer.put(src);
        return bytesToWrite;
    }

    @Override
    public long position() throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        return buffer.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        if (newPosition < 0 || newPosition > buffer.capacity()) {
            throw new IllegalArgumentException("Position out of bounds: " + newPosition);
        }
        buffer.position((int) newPosition);
        return this;
    }

    @Override
    public long size() throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        return buffer.position(); // Size is current written length, not capacity
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        if (!isOpen()) throw new IOException("Channel is closed");
        if (size < 0) throw new IllegalArgumentException("Size cannot be negative");
        if (size < buffer.position()) buffer.position((int) size);
        if (size < buffer.capacity()) {
            ByteBuffer newBuffer = ByteBuffer.allocate((int) size);
            buffer.flip();
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
        return this;
    }

    private void ensureCapacity(int requiredCapacity) {
        if (requiredCapacity > buffer.capacity()) {
            int newCapacity = Math.max(requiredCapacity, buffer.capacity() * 2);
            ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
            buffer.flip();
            newBuffer.put(buffer);
            buffer = newBuffer;
        }
    }

    public byte[] toByteArray() {
//        buffer.flip();
        buffer.rewind();
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        buffer.position(buffer.limit());
        buffer.limit(buffer.capacity());
        return data;
    }
}