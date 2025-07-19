package org.hdf5javalib.maydo.utils;

import org.hdf5javalib.maydo.hdffile.dataobjects.messages.FilterPipelineMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ByteBufferDeflaterFactory {
    private static final Map<Integer, ByteBufferDeflater> cache = new HashMap<>();

    static public ByteBufferDeflater newDeflater(FilterPipelineMessage message) {
        List<FilterPipelineMessage.FilterDescription> fds = message.getFilterDescriptions();
        List<ByteBufferDeflater> deflaters = new ArrayList<>(fds.size());

        // Build deflaters in reverse order (decompression order)
        for (int i = fds.size() - 1; i >= 0; i--) {
            FilterPipelineMessage.FilterDescription fd = fds.get(i);
            ByteBufferDeflater deflater = switch (fd.getFilterIdentification()) {
                // deflate (inflate for decompression)
                case 1 -> getOrCreate(fd.getFilterIdentification(), () -> new ZlibByteBufferDeflater(fd.getClientData()));
                // shuffle (unshuffle for decompression)
                case 2 -> getOrCreate(fd.getFilterIdentification(), () -> new ShuffleByteBufferDeflater(fd.getClientData()));
                // fletcher32
                case 3 -> getOrCreate(fd.getFilterIdentification(), () -> new Fletcher32ByteBufferDeflater(fd.getClientData()));
                // szip
                case 4 -> throw new IllegalArgumentException("szip filter not supported");
                // nbit
                case 5 -> getOrCreate(fd.getFilterIdentification(), () -> new NbitByteBufferDeflater(fd.getClientData()));
                // scaleoffset
                case 6 -> getOrCreate(fd.getFilterIdentification(), () -> new ScaleOffsetByteBufferDeflater(fd.getClientData()));
                default -> throw new IllegalArgumentException("Unknown filter identification");
            };
            deflaters.add(deflater);
        }

        if (deflaters.size() == 1) {
            return deflaters.get(0);
        } else {
            return new CompositeByteBufferDeflater(deflaters);
        }
    }

    private static ByteBufferDeflater getOrCreate(int filterId, java.util.function.Supplier<ByteBufferDeflater> creator) {
        return cache.computeIfAbsent(filterId, k -> creator.get());
    }

    private static class CompositeByteBufferDeflater implements ByteBufferDeflater {
        private final List<ByteBufferDeflater> deflaters;

        public CompositeByteBufferDeflater(List<ByteBufferDeflater> deflaters) {
            this.deflaters = deflaters;
        }

        @Override
        public ByteBuffer deflate(ByteBuffer input) throws IOException {
            ByteBuffer current = input;
            for (ByteBufferDeflater deflater : deflaters) {
                current = deflater.deflate(current);
            }
            return current;
        }
    }
}