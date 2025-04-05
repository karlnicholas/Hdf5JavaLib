package org.hdf5javalib.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.DataLayoutMessage;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import org.hdf5javalib.file.metadata.HdfSuperblock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

@Getter
@Slf4j
public class HdfFile {
    private final String fileName;
    private final StandardOpenOption[] openOptions;
    // initial setup without Dataset
    private final HdfSuperblock superblock;
    private final HdfGroup rootGroup;
    private final HdfFileAllocation fileAllocation;
    private final HdfGlobalHeap globalHeap;

    public HdfFile(String fileName, StandardOpenOption[] openOptions) {
        this.fileName = fileName;
        this.openOptions = openOptions;
        this.fileAllocation = new HdfFileAllocation();
        // this.globalHeap = new HdfGlobalHeap(bufferAllocation::getGlobalHeapAddress);
        this.globalHeap = new HdfGlobalHeap(()->fileAllocation.getGlobalHeapOffset());

        // 100320
        superblock = new HdfSuperblock(0, 0, 0, 0,
                (short)8, (short)8,
                4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                // HdfFixedPoint.of(bufferAllocation.getDataAddress()),
                HdfFixedPoint.of(fileAllocation.getDataOffset()),
                HdfFixedPoint.undefined((short)8),
                new HdfSymbolTableEntry(
                        HdfFixedPoint.of(0),
                        // HdfFixedPoint.of(bufferAllocation.getObjectHeaderPrefixAddress()),
                        HdfFixedPoint.of(fileAllocation.getRootObjectHeaderPrefixOffset()),
                        // HdfFixedPoint.of(bufferAllocation.getBtreeAddress()),
                        HdfFixedPoint.of(fileAllocation.getRootBtreeOffset()),
                        // HdfFixedPoint.of(bufferAllocation.getLocalHeapAddress())));
                        HdfFixedPoint.of(fileAllocation.getRootLocalHeapOffset())));

        // rootGroup = new HdfGroup(this, "", bufferAllocation.getBtreeAddress(), bufferAllocation.getLocalHeapAddress());
        rootGroup = new HdfGroup(this, "", fileAllocation.getRootBtreeOffset(), fileAllocation.getRootLocalHeapOffset());
    }

    /**
     * by default, the root group.
     * @param datasetName String
     * @param hdfDatatype HdfDatatype
     * @param dataSpaceMessage DataspaceMessage
     * @return HdfDataSet
     */
    public HdfDataSet createDataSet(String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        hdfDatatype.setGlobalHeap(globalHeap);
        // return rootGroup.createDataSet(datasetName, hdfDatatype, dataSpaceMessage, bufferAllocation.getDataGroupAddress());
        return rootGroup.createDataSet(datasetName, hdfDatatype, dataSpaceMessage, fileAllocation.getInitialObjectHeaderOffset());
    }

    protected void recomputeGlobalHeapAddress(HdfDataSet dataSet) {
        HdfFixedPoint dimensionSize = dataSet.getDataObjectHeaderPrefix().findMessageByType(DataLayoutMessage.class).orElseThrow().getDimensionSizes()[0];
        // bufferAllocation.computeGlobalHeapAddress(dimensionSize.getInstance(Long.class));
        fileAllocation.computeGlobalHeapOffset(dimensionSize.getInstance(Long.class));
    }

    public long write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            // fileChannel.position(bufferAllocation.getDataAddress());
            fileChannel.position(fileAllocation.getDataOffset());
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
            }
        }
        // return bufferAllocation.getDataAddress();
        return fileAllocation.getDataOffset();
    }

    public long write(ByteBuffer buffer, HdfDataSet hdfDataSet) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            // fileChannel.position(bufferAllocation.getDataAddress());
            fileChannel.position(fileAllocation.getDataOffset());
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        }
        // return bufferAllocation.getDataAddress();
        return fileAllocation.getDataOffset();
    }

    public void close() throws IOException {
        // long endOfFileAddress = bufferAllocation.getDataAddress();
        long endOfFileAddress = fileAllocation.getDataOffset();
        HdfFixedPoint[] dimensionSizes = rootGroup.getDataSet().getDataObjectHeaderPrefix()
                .findMessageByType(DataLayoutMessage.class)
                .orElseThrow()
                .getDimensionSizes();
        for(HdfFixedPoint fixedPoint : dimensionSizes) {
            endOfFileAddress += fixedPoint.getInstance(Long.class);
        }

        // some convoluted logic for adding globalHeap data if needed
        ByteBuffer globalHeapBuffer = null;
        long globalHeapAddress = -1;
        long globalHeapSize = globalHeap.getWriteBufferSize();
        if ( globalHeapSize > 0 ) {
            globalHeapAddress = endOfFileAddress;
            endOfFileAddress += globalHeapSize;
            globalHeapBuffer = ByteBuffer.allocate((int) globalHeapSize);
            globalHeap.writeToByteBuffer(globalHeapBuffer);
            globalHeapBuffer.position(0);
        }
        superblock.setEndOfFileAddress(HdfFixedPoint.of(endOfFileAddress));


        log.debug("{}", superblock);
        log.debug("{}", rootGroup);

        // Allocate the buffer dynamically up to the data start location
        // ByteBuffer buffer = ByteBuffer.allocate((int) bufferAllocation.getDataAddress()).order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian
        ByteBuffer buffer = ByteBuffer.allocate((int) fileAllocation.getDataOffset()).order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian
        // buffer.position((int) bufferAllocation.getSuperblockAddress());
        buffer.position((int) fileAllocation.getSuperblockOffset());
        superblock.writeToByteBuffer(buffer);
        // buffer.position((int) bufferAllocation.getObjectHeaderPrefixAddress());
        buffer.position((int) fileAllocation.getRootObjectHeaderPrefixOffset());
        rootGroup.writeToBuffer(buffer);
        buffer.position(0);

        Path path = Path.of(fileName);
        StandardOpenOption[] fileOptions = {StandardOpenOption.WRITE};
        if ( !Files.exists(path) ) {
            fileOptions =new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE};
        }
        try (FileChannel fileChannel = FileChannel.open(path, fileOptions)) {
            fileChannel.position(0);

            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
            // check here if global heap needs to be written
            if ( globalHeapAddress > 0 ) {
                fileChannel.position(globalHeapAddress);
                while (globalHeapBuffer.hasRemaining()) {
                    fileChannel.write(globalHeapBuffer);
                }
            }
        }
    }

}
