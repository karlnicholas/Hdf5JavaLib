package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class HdfGlobalHeapGrok {
    private static final String SIGNATURE = "GCOL";
    private static final int VERSION = 1;

    private HdfFixedPoint collectionSize;
    private HdfFixedPoint dataSegmentAddress;
    private List<GlobalHeapObject> objects;
    private int nextObjectId;
    public GlobalHeapInitialize initialize;

    public HdfGlobalHeapGrok(GlobalHeapInitialize initialize) {
        this.initialize = initialize;
        this.collectionSize = HdfFixedPoint.of(4096L);
        this.objects = null;
        this.nextObjectId = 1;
    }

    public byte[] getDataBytes(int length, long offset, int index) {
        if (objects == null) {
            initialize.initializeCallback(length, offset, index);
        }
        GlobalHeapObject obj = objects.get(index - 1);
        if ( obj.objectId != index) {
            throw new RuntimeException("Invalid index");
        }
        return obj.getData();
    }

    public int addToHeap(HdfString objectName) {
        if (objects == null) {
            objects = new ArrayList<>();
        }
        byte[] objectData = objectName.getBytes();
        int objectSize = objectData.length;
        int alignedSize = (objectSize + 7) & ~7;
        int headerSize = 16;

        if (nextObjectId > 0xFFFF) {
            throw new IllegalStateException("Maximum number of global heap objects exceeded.");
        }

        long newSize = collectionSize.getInstance(Long.class) + headerSize + alignedSize;
        this.collectionSize = HdfFixedPoint.of(newSize);

        GlobalHeapObject obj = new GlobalHeapObject(nextObjectId, 1, objectSize, objectData);
        objects.add(obj);
        int objectId = nextObjectId;
        nextObjectId++;
        return objectId;
    }

    public void readFromFileChannel(FileChannel fileChannel, short offsetSize) throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(16);
        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerBuffer);
        headerBuffer.flip();

        byte[] signatureBytes = new byte[4];
        headerBuffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!SIGNATURE.equals(signature)) {
            throw new IllegalArgumentException("Invalid global heap signature: " + signature);
        }

        int version = Byte.toUnsignedInt(headerBuffer.get());
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported global heap version: " + version);
        }

        headerBuffer.position(headerBuffer.position() + 3);
        HdfFixedPoint collectionSize = HdfFixedPoint.readFromByteBuffer(headerBuffer, (short) 8, new BitSet(), (short) 0, (short) 64);
        long declaredSize = collectionSize.getInstance(Long.class);

        ByteBuffer objectBuffer = ByteBuffer.allocate((int) (declaredSize - 16));
        objectBuffer.order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(objectBuffer);
        objectBuffer.flip();

        List<GlobalHeapObject> objects = new ArrayList<>();
        int nextObjectId = 1;

        while (objectBuffer.position() < objectBuffer.limit()) {
            GlobalHeapObject obj = GlobalHeapObject.readFromByteBuffer(objectBuffer);
            objects.add(obj);
            if (obj.getObjectId() == 0) {
                break; // Object 0 is free space, stop
            }
            nextObjectId = Math.max(nextObjectId, obj.getObjectId() + 1);
        }

        this.collectionSize = collectionSize;
        this.dataSegmentAddress = HdfFixedPoint.of(fileChannel.position() - (declaredSize - 16) - 16);
        this.objects = objects;
        this.nextObjectId = nextObjectId;
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
        if (objects == null) {
            throw new IllegalStateException("Heap not initialized; cannot write.");
        }
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(SIGNATURE.getBytes());
        buffer.put((byte) VERSION);
        buffer.put(new byte[3]);
        long totalSize = 16;
        for (GlobalHeapObject obj : objects) {
            totalSize += 16 + obj.getObjectSize() + (8 - (obj.getObjectSize() % 8)) % 8;
        }
        buffer.putLong(totalSize);

        for (GlobalHeapObject obj : objects) {
            obj.writeToByteBuffer(buffer);
        }
    }

    @Override
    public String toString() {
        return "HdfGlobalHeapGrok{" +
                "signature='" + SIGNATURE + '\'' +
                ", version=" + VERSION +
                ", collectionSize=" + collectionSize +
                ", dataSegmentAddress=" + dataSegmentAddress +
                ", objects=" + (objects != null ? objects.size() : "null") +
                '}';
    }

    public interface GlobalHeapInitialize {
        void initializeCallback(int length, long offset, int index);
    }

    @Getter
    private static class GlobalHeapObject {
        private final int objectId;
        private final int referenceCount;
        private final int objectSize;
        private final byte[] data;

        public GlobalHeapObject(int objectId, int referenceCount, int objectSize, byte[] data) {
            this.objectId = objectId;
            this.referenceCount = referenceCount;
            this.objectSize = objectSize;
            this.data = data;
        }

        public static GlobalHeapObject readFromByteBuffer(ByteBuffer buffer) {
            int objectId = Short.toUnsignedInt(buffer.getShort());
            int referenceCount = Short.toUnsignedInt(buffer.getShort());
            buffer.getInt();
            int objectSize = (int) buffer.getLong();

            byte[] data = (objectId == 0) ? new byte[0] : new byte[objectSize];
            if (objectId != 0) {
                buffer.get(data);
                buffer.position(buffer.position() + (8 - (objectSize % 8)) % 8);
            }
            return new GlobalHeapObject(objectId, referenceCount, objectSize, data);
        }

        public void writeToByteBuffer(ByteBuffer buffer) {
            buffer.putShort((short) objectId);
            buffer.putShort((short) referenceCount);
            buffer.putInt(0);
            buffer.putLong(objectSize);
            if (objectId != 0) {
                buffer.put(data);
                int padding = (8 - (objectSize % 8)) % 8;
                if (padding > 0) {
                    buffer.put(new byte[padding]);
                }
            }
        }
    }
}