package org.hdf5javalib.maydo.utils;

import org.hdf5javalib.maydo.hdffile.dataobjects.messages.FilterPipelineMessage;

import java.util.HashMap;
import java.util.Map;

public class ByteBufferDeflaterFactory {
    private static final Map<Integer, ByteBufferDeflater> cache = new HashMap<>();

    static public ByteBufferDeflater newDeflater(FilterPipelineMessage message) {
        if (message.getNumberOfFilters() > 1) {
            throw new IllegalArgumentException("Only one filter can be deflated");
        }
        FilterPipelineMessage.FilterDescription fd = message.getFilterDescriptions().get(0);
        return switch (fd.getFilterIdentification()) {  // Fixed typo: Indentification -> Identification
            // deflate
            case 1 -> {
                int level = fd.getClientData()[0];
                yield cache.computeIfAbsent(level, ZlibByteBufferDeflater::new);
            }
            // shuffle
            case 2 -> throw new IllegalArgumentException("shuffle filter not supported");
            // fletcher32
            case 3 -> throw new IllegalArgumentException("fletcher32 filter not supported");
            // szip
            case 4 -> throw new IllegalArgumentException("szip filter not supported");
            // nbit
            case 5 -> throw new IllegalArgumentException("nbit filter not supported");
            // scaleoffset
            case 6 -> throw new IllegalArgumentException("scaleoffset filter not supported");
            default -> throw new IllegalArgumentException("Unknown filter identification");  // Fixed typo: indentification -> identification
        };
    }
}