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

import static org.hdf5javalib.file.HdfFileAllocation.AllocationType;

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
        this.globalHeap = new HdfGlobalHeap(()->fileAllocation.getAllocation(AllocationType.GLOBAL_HEAP).getAddress());

        // 100320
        superblock = new HdfSuperblock(0, 0, 0, 0,
                (short)8, (short)8,
                4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.of(fileAllocation.getAllocation(AllocationType.DATA_SEGMENT).getAddress()),
                HdfFixedPoint.undefined((short)8),
                new HdfSymbolTableEntry(
                        HdfFixedPoint.of(0),
                        HdfFixedPoint.of(fileAllocation.getAllocation(AllocationType.OBJECT_HEADER_PREFIX).getAddress()),
                        HdfFixedPoint.of(fileAllocation.getAllocation(AllocationType.BTREE).getAddress()),
                        HdfFixedPoint.of(fileAllocation.getAllocation(AllocationType.LOCAL_HEAP).getAddress())));

        rootGroup = new HdfGroup(this, "", fileAllocation.getAllocation(AllocationType.BTREE).getAddress(), fileAllocation.getAllocation(AllocationType.LOCAL_HEAP).getAddress());
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
        return rootGroup.createDataSet(datasetName, hdfDatatype, dataSpaceMessage, fileAllocation.getAllocation(AllocationType.DATA_GROUP).getAddress());
    }

    protected void recomputeGlobalHeapAddress(HdfDataSet dataSet) {
        HdfFixedPoint dimensionSize = dataSet.getDataObjectHeaderPrefix().findMessageByType(DataLayoutMessage.class).orElseThrow().getDimensionSizes()[0];
        fileAllocation.computeGlobalHeap(dimensionSize.getInstance(Long.class));
    }

    public long write(Supplier<ByteBuffer> bufferSupplier) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            fileChannel.position(fileAllocation.getAllocation(AllocationType.DATA_SEGMENT).getAddress());
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
            }
        }
        return fileAllocation.getAllocation(AllocationType.DATA_SEGMENT).getAddress();
    }

    public long write(ByteBuffer buffer, HdfDataSet hdfDataSet) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            fileChannel.position(fileAllocation.getAllocation(AllocationType.DATA_SEGMENT).getAddress());
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        }
        return fileAllocation.getAllocation(AllocationType.DATA_SEGMENT).getAddress();
    }

    public void close() throws IOException {
        long endOfFileAddress = fileAllocation.getLastAddressUsed();
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
        ByteBuffer buffer = ByteBuffer.allocate((int) fileAllocation.getAllocation(AllocationType.DATA_SEGMENT).getAddress()).order(ByteOrder.LITTLE_ENDIAN); // HDF5 uses little-endian
        buffer.position((int) fileAllocation.getAllocation(AllocationType.SUPERBLOCK).getAddress());
        superblock.writeToByteBuffer(buffer);
        buffer.position((int) fileAllocation.getAllocation(AllocationType.OBJECT_HEADER_PREFIX, 0).getAddress());
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
