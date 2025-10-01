package org.hdf5javalib.hdffile.infrastructure.v2btree;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Type5Record implements BTreeV2Record { // Link Name
    public final int nameHash;
    public final byte[] heapId; // 7 bytes

    private Type5Record(ByteBuffer bb) {
        this.nameHash = bb.getInt();
        this.heapId = new byte[7];
        bb.get(this.heapId);
    }

    public static Type5Record read(ByteBuffer bb) {
        return new Type5Record(bb);
    }

    @Override
    public String toString() {
        return "Type5Record{nameHash=" + nameHash + ", heapId=" + Arrays.toString(heapId) + '}';
    }
}

