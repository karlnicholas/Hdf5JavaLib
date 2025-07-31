package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfReadUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeapBlock.GLOBAL_HEAP_OBJECT_SIZE;

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
    public byte[] getDataBytes(HdfFixedPoint heapOffset, int objectId) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
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
        return block.getDataBytes(heapOffset, objectId);
    }

    /**
     * Reads a global heap collection from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @throws IOException if an I/O error occurs or the heap data is invalid
     */
    public void initializeFromSeekableByteChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
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
        HdfFixedPoint localCollectionSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), headerBuffer);
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
                        throw new IllegalStateException("Duplicate object ID " + obj.getHeapObjectIndex() + " found in heap at offset: " + startOffset);
                    }
                    if (obj.getHeapObjectIndex() < 1 || obj.getHeapObjectIndex() > 0xFFFF) {
                        throw new IllegalStateException("Invalid object ID " + obj.getHeapObjectIndex() + " found in heap at offset: " + startOffset);
                    }
                    localObjects.put(obj.getHeapObjectIndex(), obj);
                    localNextObjectId = Math.max(localNextObjectId, obj.getHeapObjectIndex() + 1);
                }
            }
        } catch (Exception e) {
            throw new IOException("Unexpected error processing global heap object data buffer at offset: " + startOffset, e);
        }

        HdfFixedPoint hdfStartOffset = HdfWriteUtils.hdfFixedPointFromValue(startOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset());

        HdfGlobalHeapBlock heapBlock = new HdfGlobalHeapBlock(
                localObjects,
                localNextObjectId,
                hdfDataFile,
                hdfStartOffset
        );
        globalHeaps.put(hdfStartOffset, heapBlock);
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
        return null;
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
        void initializeCallback(HdfFixedPoint heapOffset) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException;
    }
}