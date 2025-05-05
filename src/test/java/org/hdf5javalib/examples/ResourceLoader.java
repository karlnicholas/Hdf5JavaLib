package org.hdf5javalib.examples;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Utility class for loading resources into a MemorySeekableByteChannel.
 */
public class ResourceLoader {

    /**
     * Loads  * Loads a resource from the classpath into a MemorySeekableByteChannel.
     *
     * @param resourcePath the path to the resource (e.g., "data/testfile.h5")
     * @return a MemorySeekableByteChannel containing the resource data
     * @throws IOException if the resource is not found or cannot be read
     */
    public static SeekableByteChannel loadResourceAsChannel(String resourcePath) throws IOException {
        try (InputStream inputStream = ResourceLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourcePath);
            }

            // Read the resource into a byte array
            byte[] data = inputStream.readAllBytes();

            // Create a MemorySeekableByteChannel with sufficient capacity
            MemorySeekableByteChannel channel = new MemorySeekableByteChannel(data.length);
            channel.write(ByteBuffer.wrap(data));

            // Reset position to the beginning
            channel.position(0);
            return channel;
        }
    }
}