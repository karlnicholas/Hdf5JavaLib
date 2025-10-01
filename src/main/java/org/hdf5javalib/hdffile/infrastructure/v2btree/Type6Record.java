package org.hdf5javalib.hdffile.infrastructure.v2btree;

import java.nio.ByteBuffer;
import java.util.Arrays;

class Type6Record implements BTreeV2Record { // Creation Order
    public final long creationOrder;
    public final byte[] heapId; // 7 bytes

    private Type6Record(ByteBuffer bb) {
        this.creationOrder = bb.getLong();
        this.heapId = new byte[7];
        bb.get(this.heapId);
    }

    public static Type6Record read(ByteBuffer bb) {
        return new Type6Record(bb);
    }

    @Override
    public String toString() {
        return "Type6Record{creationOrder=" + creationOrder + ", heapId=" + Arrays.toString(heapId) + '}';
    }
}

