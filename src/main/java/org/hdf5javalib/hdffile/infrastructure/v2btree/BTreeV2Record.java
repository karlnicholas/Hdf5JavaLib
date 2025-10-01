package org.hdf5javalib.hdffile.infrastructure.v2btree;

import java.nio.ByteBuffer;

// --- Record Abstraction and Implementations ---
public interface BTreeV2Record {
    static BTreeV2Record read(ByteBuffer bb, BTreeV2Header header) {
        // Factory method to read the correct record type
        switch (header.type) {
            case GROUP_LINK_NAME:
                return Type5Record.read(bb);
            case GROUP_LINK_CREATION_ORDER:
                return Type6Record.read(bb);
            case ATTRIBUTE_NAME:
                return Type8Record.read(bb);
            // Add other types as needed
            default:
                // For unsupported types, we can skip the bytes
                bb.position(bb.position() + header.recordSize);
                return new UnsupportedRecord(header.type);
        }
    }
}

