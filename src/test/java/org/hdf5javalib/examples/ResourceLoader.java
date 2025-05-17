package org.hdf5javalib.examples;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;

/**
 * Utility class for loading resources into a MemorySeekableByteChannel.
 */
public class ResourceLoader {

    /**
     * Loads  * Loads a resource from the classpath into a MemorySeekableByteChannel.
     *
     * @param resourceName the path to the resource (e.g., "data/testfile.h5")
     * @return a MemorySeekableByteChannel containing the resource data
     * @throws IOException if the resource is not found or cannot be read
     */
    public static SeekableByteChannel loadResourceAsChannel(String resourceName) throws IOException {
        String absPath = new File("src/test/resources/" + resourceName).getAbsolutePath();

        // Read the resource into a byte array
        byte[] data = Files.readAllBytes(new File(absPath).toPath());

        // Create a MemorySeekableByteChannel with sufficient capacity
        MemorySeekableByteChannel channel = new MemorySeekableByteChannel(data.length);
        channel.write(ByteBuffer.wrap(data));

        // Reset position to the beginning
        channel.position(0);
        return channel;
    }
}