package org.hdf5javalib.hdffile.infrastructure.v2btree.gemini;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Type8Record implements BTreeV2Record { // Creation Order
    public final byte[] heapId; // 7 bytes
    public final int messageFlags;
    public final int creationOrder;
    public final int hashOfName;

    private Type8Record(ByteBuffer bb) {
        this.heapId = new byte[8];
        bb.get(this.heapId);
        this.messageFlags = bb.get();
        this.creationOrder = bb.getInt();
        this.hashOfName = bb.getInt();
    }

    public static Type8Record read(ByteBuffer bb) {
        return new Type8Record(bb);
    }

    @Override
    public String toString() {
        return "Type8Record{heapId=" + Arrays.toString(heapId)
                + ", messageFlags=" + messageFlags
                + ", creationOrder=" + creationOrder
                + ", hashOfName=" + hashOfName + '}';
    }
}

