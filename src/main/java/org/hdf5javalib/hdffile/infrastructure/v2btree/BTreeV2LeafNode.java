package org.hdf5javalib.hdffile.infrastructure.v2btree;

import java.nio.ByteBuffer;

class BTreeV2LeafNode extends BTreeV2Node {
    public final int checksum;

    private BTreeV2LeafNode(ByteBuffer bb, BTreeV2Header header, int recordsInThisNode) {
        super("BTLF", bb, header, recordsInThisNode);
        this.checksum = Hdf5Utils.readChecksum(bb);
    }

    public static BTreeV2LeafNode read(ByteBuffer bb, BTreeV2Header header, int recordsInThisNode) {
        return new BTreeV2LeafNode(bb, header, recordsInThisNode);
    }
}

