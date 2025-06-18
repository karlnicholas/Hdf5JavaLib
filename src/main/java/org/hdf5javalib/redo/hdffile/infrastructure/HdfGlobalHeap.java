package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.hdffile.AllocationType;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.HdfFileAllocation;
import org.hdf5javalib.redo.utils.HdfReadUtils;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hdf5javalib.redo.hdffile.infrastructure.HdfGlobalHeapBlock.GLOBAL_HEAP_OBJECT_SIZE;

/**
 * Manages HDF5 global heap collections as defined in the HDF5 specification.
 * <p>
 * The {@code HdfGlobalHeap} class handles the storage and retrieval of global heap objects
 * in an HDF5 file. It supports reading and writing heap collections, adding new objects,
 * and managing object IDs and sizes. Each heap collection is identified by its file offset
 * and contains objects with unique IDs and associated data.
 * </p>
 *
 * @see HdfDataFile
 * @see HdfFixedPoint
 * @see HdfFileAllocation
 */
public class HdfGlobalHeap {
    private static final byte[] GLOBAL_HEAP_SIGNATURE = {'G', 'C', 'O', 'L'};
    private static final int VERSION = 1;
    private static final int GLOBAL_HEAP_HEADER_SIZE = 16;
    private static final int GLOBAL_HEAP_RESERVED_1_SIZE = 3;
    /**
     * Map of heap offsets to collections of global heap objects.
     */
    private final Map<HdfFixedPoint, HdfGlobalHeapBlock> globalHeaps;
    /**
     * Optional initializer for lazy loading heap collections.
     */
    private final GlobalHeapInitialize initialize;
    /**
     * The HDF5 file context.
     */
    private final HdfDataFile hdfDataFile;
    /**
     * The current heap offset for writing new objects.
     */
    private HdfFixedPoint currentWriteHeapOffset;

    /**
     * Constructs an HdfGlobalHeap with an initializer and file context.
     *
     * @param initialize  the initializer for lazy loading heap collections
     * @param hdfDataFile the HDF5 file context
     */
    public HdfGlobalHeap(GlobalHeapInitialize initialize, HdfDataFile hdfDataFile) {
        this.initialize = initialize;
        this.hdfDataFile = hdfDataFile;
        this.globalHeaps = new HashMap<>();
        this.currentWriteHeapOffset = null;
    }

    /**
     * Constructs an HdfGlobalHeap without an initializer.
     *
     * @param hdfDataFile the HDF5 file context
     */
    public HdfGlobalHeap(HdfDataFile hdfDataFile) {
        this.hdfDataFile = hdfDataFile;
        this.initialize = null;
        this.globalHeaps = new HashMap<>();
        this.currentWriteHeapOffset = null;
    }

    /**
     * Retrieves the data bytes for a specific global heap object.
     *
     * @param heapOffset the offset of the heap collection
     * @param objectId   the ID of the object
     * @return the data bytes of the object
     * @throws IllegalArgumentException if the object ID is 0 or invalid
     * @throws IllegalStateException    if the heap or object is not found
     */
    public byte[] getDataBytes(HdfFixedPoint heapOffset, int objectId) {
        if (objectId == 0) {
            throw new IllegalArgumentException("Cannot request data bytes for Global Heap Object ID 0 (null terminator)");
        }
        HdfGlobalHeapBlock block = globalHeaps.get(heapOffset);
        if (block == null) {
            if (initialize != null) {
                initialize.initializeCallback(heapOffset);
                block = globalHeaps.get(heapOffset);
                if (block == null) {
                    throw new IllegalStateException("Heap not found or loaded for offset: " + heapOffset + " even after initialization callback.");
                }
            } else {
                throw new IllegalStateException("Heap not loaded for offset: " + heapOffset + " and no initializer provided.");
            }
        }
//        LinkedHashMap<Integer, GlobalHeapObject> specificHeapObjects = heapCollections.get(heapOffset);
        return block.getDataBytes(heapOffset, objectId);
    }

    /**
     * Reads a global heap collection from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @throws IOException if an I/O error occurs or the heap data is invalid
     */
    public void initializeFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws IOException {
        long startOffset = fileChannel.position();
        ByteBuffer headerBuffer = ByteBuffer.allocate(GLOBAL_HEAP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerBuffer);
        headerBuffer.flip();

        byte[] signatureBytes = new byte[GLOBAL_HEAP_SIGNATURE.length];
        headerBuffer.get(signatureBytes);
        if (Arrays.compare(GLOBAL_HEAP_SIGNATURE, signatureBytes) != 0) {
            throw new IllegalArgumentException("Invalid global heap signature: '" + Arrays.toString(signatureBytes) + "' at offset: " + startOffset);
        }

        int version = Byte.toUnsignedInt(headerBuffer.get());
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported global heap version: " + version + " at offset: " + startOffset);
        }

        headerBuffer.position(headerBuffer.position() + GLOBAL_HEAP_RESERVED_1_SIZE);
        HdfFixedPoint localCollectionSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength(), headerBuffer);
        long declaredSize = localCollectionSize.getInstance(Long.class);

        int objectDataBufferSize = (int) (declaredSize - GLOBAL_HEAP_OBJECT_SIZE);

        ByteBuffer objectBuffer = null;

        if (objectDataBufferSize > 0) {
            objectBuffer = ByteBuffer.allocate(objectDataBufferSize).order(ByteOrder.LITTLE_ENDIAN);
            fileChannel.read(objectBuffer);
            objectBuffer.flip();
        }

        LinkedHashMap<Integer, HdfGlobalHeapBlock.GlobalHeapObject> localObjects = new LinkedHashMap<>();
        int localNextObjectId = 1;

        try {
            if (objectBuffer != null) {
                while (objectBuffer.hasRemaining()) {
                    if (objectBuffer.remaining() < GLOBAL_HEAP_OBJECT_SIZE ) {
                        break;
                    }
                    HdfGlobalHeapBlock.GlobalHeapObject obj = HdfGlobalHeapBlock.GlobalHeapObject.readFromByteBuffer(objectBuffer);
                    if (obj.getHeapObjectIndex() == 0) {
                        localObjects.put(obj.getHeapObjectIndex(), obj);
                        break;
                    }
                    if (localObjects.containsKey(obj.getHeapObjectIndex())) {
                        throw new RuntimeException("Duplicate object ID " + obj.getHeapObjectIndex() + " found in heap at offset: " + startOffset);
                    }
                    if (obj.getHeapObjectIndex() < 1 || obj.getHeapObjectIndex() > 0xFFFF) {
                        throw new RuntimeException("Invalid object ID " + obj.getHeapObjectIndex() + " found in heap at offset: " + startOffset);
                    }
                    localObjects.put(obj.getHeapObjectIndex(), obj);
                    localNextObjectId = Math.max(localNextObjectId, obj.getHeapObjectIndex() + 1);
                }
            }
        } catch (Exception e) {
            throw new IOException("Unexpected error processing global heap object data buffer at offset: " + startOffset, e);
        }

        HdfFixedPoint hdfStartOffset = HdfWriteUtils.hdfFixedPointFromValue(startOffset, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset());

        HdfGlobalHeapBlock heapBlock = new HdfGlobalHeapBlock(
                localObjects,
                localCollectionSize,
                localNextObjectId,
                hdfDataFile,
                globalHeaps.size() > 0 ? AllocationType.GLOBAL_HEAP_2 : AllocationType.GLOBAL_HEAP_1,
                globalHeaps.size() > 0 ? "Global Heap 2" : "Global Heap 1",
                hdfStartOffset,
                hdfDataFile.getFileAllocation()
        );
        globalHeaps.put(hdfStartOffset, heapBlock);
//        this.heapCollections.put(hdfStartOffset, localObjects);
//        this.collectionSizes.put(hdfStartOffset, localCollectionSize);
//        this.nextObjectIds.put(hdfStartOffset, localNextObjectId);

//        this.setType(heapCollections.size() > 1 ? AllocationType.GLOBAL_HEAP_2 : AllocationType.GLOBAL_HEAP_1);
//        this.setName(heapCollections.size() > 1 ? "Global Heap 1" : "Global Heap 2");
//        this.setOffset(hdfStartOffset);
//        this.setSize(localCollectionSize);
//        this.hdfDataFile.getFileAllocation().addAllocationBlock(this);

    }

    /**
     * Adds a byte array to the global heap and returns a reference to it.
     *
     * @param bytes the byte array to add
     * @return a byte array containing the heap offset and object ID
     * @throws IllegalArgumentException if the input byte array is null
     * @throws IllegalStateException    if the heap block is not allocated or full
     */
    public byte[] addToHeap(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Input byte array cannot be null.");
        }

        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();

        if (currentWriteHeapOffset == null) {
            this.currentWriteHeapOffset = fileAllocation.getGlobalHeapOffset();
        }
        final HdfFixedPoint currentHeapOffset = this.currentWriteHeapOffset;

        HdfGlobalHeapBlock heapBlock = globalHeaps.computeIfAbsent(currentHeapOffset, k -> {
            return new HdfGlobalHeapBlock(
                    new LinkedHashMap<>(),
                    HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength()),
                    1,
                    hdfDataFile,
                    globalHeaps.size() > 1 ? AllocationType.GLOBAL_HEAP_2 : AllocationType.GLOBAL_HEAP_1,
                    globalHeaps.size() > 1 ? "Global Heap 2" : "Global Heap 2",
                    currentHeapOffset,
                    hdfDataFile.getFileAllocation());
//            globalHeaps.put(currentHeapOffset, heapBlock);
//            return heapBlock;

        });
        boolean attempt = heapBlock.attemptAddBytes(bytes);
        if (!attempt) {
//            currentUsedSize + newObjectRequiredSize + GLOBAL_HEAP_OBJECT_SIZE > blockSize.getInstance(Long.class);
            HdfFixedPoint newHeapOffset;
            if (currentHeapOffset == fileAllocation.getGlobalHeapOffset()) {
                newHeapOffset = fileAllocation.allocateNextGlobalHeapBlock();
            } else {
                newHeapOffset = fileAllocation.expandGlobalHeapBlock();
            }
            HdfGlobalHeapBlock globalHeapBlock = new HdfGlobalHeapBlock(
                    new LinkedHashMap<>(),
                    HdfWriteUtils.hdfFixedPointFromValue(0, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForLength()),
                    1,
                    hdfDataFile,
                    globalHeaps.size() > 1 ? AllocationType.GLOBAL_HEAP_2 : AllocationType.GLOBAL_HEAP_1,
                    globalHeaps.size() > 1 ? "Global Heap 1" : "Global Heap 1",
                    currentHeapOffset,
                    hdfDataFile.getFileAllocation());
            globalHeaps.put(newHeapOffset, globalHeapBlock);
            currentWriteHeapOffset = newHeapOffset;
        }

        return heapBlock.addToHeap(bytes);
    }

    /**
     * Interface for initializing global heap collections lazily.
     */
    public interface GlobalHeapInitialize {
        /**
         * Callback to initialize a heap collection at the specified offset.
         *
         * @param heapOffset the offset of the heap collection
         */
        void initializeCallback(HdfFixedPoint heapOffset);
    }
}