package org.hdf5javalib.hdffile.infrastructure.v2btree.gemini;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// --- Node Abstraction ---
abstract class BTreeV2Node {
    public final String signature;
    public final short version;
    public final BTreeV2Type type;
    public final List<BTreeV2Record> records;

    protected BTreeV2Node(String signature, ByteBuffer bb, BTreeV2Header header, int recordsInThisNode) {
        this.signature = signature;
        this.version = bb.get();
        this.type = BTreeV2Type.from(bb.get());
        if (this.type != header.type) {
            throw new IllegalStateException("Node type mismatch. Header: " + header.type + ", Node: " + this.type);
        }
        this.records = new ArrayList<>(recordsInThisNode);
        for (int i = 0; i < recordsInThisNode; i++) {
            records.add(BTreeV2Record.read(bb, header));
        }
    }
}

