package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfBTreeV1;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfLocalHeap;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfLocalHeapContents;
import com.github.karlnicholas.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import com.github.karlnicholas.hdf5javalib.message.SymbolTableMessage;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Getter
public class HdfFile {
    private final String fileName;
    private final StandardOpenOption[] openOptions;
    private final HdfGroupManager hdfGroupManager;
    private final AtomicLong messageCount;
    // initial setup without Dataset
    private HdfSuperblock superblock;
    private HdfSymbolTableEntry rootGroupEntry;
    private HdfObjectHeaderPrefixV1 objectHeaderPrefix;
    private HdfLocalHeap localHeap;
    private HdfLocalHeapContents localHeapContents;
    private HdfBTreeV1 bTree;

    public HdfFile(String fileName, StandardOpenOption[] openOptions) {
        this.fileName = fileName;
        this.openOptions = openOptions;
        hdfGroupManager = new HdfGroupManager(this);

        // 100320
        superblock = new HdfSuperblock(0, 0, 0, 0, (short)8, (short)8, 4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.of(800),
                HdfFixedPoint.undefined((short)8));
        // Define a root group
        rootGroupEntry = new HdfSymbolTableEntry(HdfFixedPoint.of(0), HdfFixedPoint.of(96), 1,
                HdfFixedPoint.of(136),
                HdfFixedPoint.of(680));

        objectHeaderPrefix = new HdfObjectHeaderPrefixV1(1, 1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(
                        HdfFixedPoint.of(136),
                        HdfFixedPoint.of(680))));

        // Define the heap data size, why 88 I don't know.
        int dataSegmentSize = 8 + 8*10; // allow for 10 simple entries, or whatever aligned to 8 bytes
        // Initialize the heapData array
        byte[] heapData = new byte[dataSegmentSize];
        Arrays.fill(heapData, (byte) 0); // Set all bytes to 0

        localHeap = new HdfLocalHeap(HdfFixedPoint.of(dataSegmentSize), HdfFixedPoint.of(712));
        localHeapContents = new HdfLocalHeapContents(new byte[dataSegmentSize]);
        localHeap.addToHeap(new HdfString(new byte[0], false, false), this.localHeapContents);

        // Define a B-Tree for group indexing
        bTree = new HdfBTreeV1("TREE", 0, 0, 0,
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.undefined((short)8));

        messageCount = new AtomicLong();

    }

    public <T> HdfDataSet<T> createDataSet(String datasetName, CompoundDataType compoundType, HdfFixedPoint[] hdfDimensions) {
        return hdfGroupManager.createDataSet(this, datasetName, compoundType, hdfDimensions);
    }

    public void write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet<?> hdfDataSet) throws IOException {
        messageCount.set(0);
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            long dataAddress = hdfDataSet.getDatasetAddress().getBigIntegerValue().longValue();
            fileChannel.position(dataAddress);
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                messageCount.incrementAndGet();
                fileChannel.write(buffer);
            }
        }
    }

    public <T> void closeDataset(HdfDataSet<T> hdfDataSet) throws IOException {
        hdfGroupManager.closeDataSet(hdfDataSet, messageCount.get());
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), StandardOpenOption.WRITE)) {
            fileChannel.position(0);
            hdfGroupManager.writeToFile(fileChannel, hdfDataSet);
        }
    }

    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), StandardOpenOption.WRITE)) {
            // TODO: Implement actual serialization logic
            //        System.out.println("Superblock: " + superblock);
            // Allocate a buffer of size 2208
            // Get the data address directly from the single dataObject
//            HdfSuperblock superblock = hdfDataSet.getHdfFile().getSuperblock();
//            Optional<HdfFixedPoint> optionalDataAddress = dataObjectHeaderPrefix.getDataAddress();

//            // Extract the data start location dynamically
//            long dataStart = optionalDataAddress
//                    .map(HdfFixedPoint::getBigIntegerValue)
//                    .map(BigInteger::longValue)
//                    .orElseThrow(() -> new IllegalStateException("No Data Layout Message found"));
            int dataStart = 0;
            if ( bTree.getEntriesUsed() <= 0 ) {
                dataStart = superblock.getEndOfFileAddress().getBigIntegerValue().intValue();
            }

            // Allocate the buffer dynamically up to the data start location
            ByteBuffer buffer = ByteBuffer.allocate(dataStart).order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian

            //        System.out.println(superblock);
            // Write the superblock at position 0
            buffer.position(0);
            superblock.writeToByteBuffer(buffer);

            System.out.println(rootGroupEntry);
            // Write the root group symbol table entry immediately after the superblock
            rootGroupEntry.writeToByteBuffer(buffer, superblock.getSizeOfOffsets());

            System.out.println(objectHeaderPrefix);
            // Write Object Header at position found in rootGroupEntry
            int objectHeaderAddress = rootGroupEntry.getObjectHeaderAddress().getBigIntegerValue().intValue();
            buffer.position(objectHeaderAddress);
            objectHeaderPrefix.writeToByteBuffer(buffer);

            long localHeapPosition = -1;
            long bTreePosition = -1;

            // Try getting the Local Heap Address from the Root Symbol Table Entry
            if (rootGroupEntry.getLocalHeapAddress() != null && !rootGroupEntry.getLocalHeapAddress().isUndefined()) {
                localHeapPosition = rootGroupEntry.getLocalHeapAddress().getBigIntegerValue().longValue();
            }

            // If not found or invalid, fallback to Object Header's SymbolTableMessage
            Optional<SymbolTableMessage> symbolTableMessageOpt = objectHeaderPrefix.findHdfSymbolTableMessage(SymbolTableMessage.class);
            if (symbolTableMessageOpt.isPresent()) {
                SymbolTableMessage symbolTableMessage = symbolTableMessageOpt.get();

                // Retrieve Local Heap Address if still not found
                if (localHeapPosition == -1 && symbolTableMessage.getLocalHeapAddress() != null && !symbolTableMessage.getLocalHeapAddress().isUndefined()) {
                    localHeapPosition = symbolTableMessage.getLocalHeapAddress().getBigIntegerValue().longValue();
                }

                // Retrieve B-Tree Address
                if (symbolTableMessage.getBTreeAddress() != null && !symbolTableMessage.getBTreeAddress().isUndefined()) {
                    bTreePosition = symbolTableMessage.getBTreeAddress().getBigIntegerValue().longValue();
                }
            }

            // Validate B-Tree Position and write it
            if (bTreePosition != -1) {
                System.out.println(bTree);
                buffer.position((int) bTreePosition); // Move to the correct position
                bTree.writeToByteBuffer(buffer);
            } else {
                throw new IllegalStateException("No valid B-Tree position found.");
            }

            // Validate Local Heap Position and write it
            if (localHeapPosition != -1) {
                buffer.position((int) localHeapPosition); // Move to the correct position
                localHeap.writeToByteBuffer(buffer);
                buffer.position(localHeap.getDataSegmentAddress().getBigIntegerValue().intValue());
                localHeapContents.writeToByteBuffer(buffer);
            } else {
                throw new IllegalStateException("No valid Local Heap position found.");
            }
            fileChannel.position(0);
            fileChannel.write(buffer);
        }
    }
}
