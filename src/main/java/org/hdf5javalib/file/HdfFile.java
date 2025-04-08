package org.hdf5javalib.file;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import org.hdf5javalib.file.metadata.HdfSuperblock;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Supplier;

@Getter
@Slf4j
public class HdfFile implements Closeable {
    private final String fileName;
    private final StandardOpenOption[] openOptions;
    // initial setup without Dataset
    private final HdfSuperblock superblock;
    private final HdfGroup rootGroup;
    private final HdfGlobalHeap globalHeap;

    public HdfFile(String fileName, StandardOpenOption[] openOptions) {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        this.fileName = fileName;
        this.openOptions = openOptions;
        // this.globalHeap = new HdfGlobalHeap(bufferAllocation::getGlobalHeapAddress);
        this.globalHeap = new HdfGlobalHeap();

        // 100320
        superblock = new HdfSuperblock(0, 0, 0, 0,
                (short)8, (short)8,
                4, 16,
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                // HdfFixedPoint.of(bufferAllocation.getDataAddress()),
                // this offset of the end of the file. will need to be updated later.
                HdfFixedPoint.of(0),
                HdfFixedPoint.undefined((short)8),
                new HdfSymbolTableEntry(
                        HdfFixedPoint.of(0),
                        // HdfFixedPoint.of(bufferAllocation.getObjectHeaderPrefixAddress()),
                        HdfFixedPoint.of(fileAllocation.getObjectHeaderPrefixOffset()),
                        // HdfFixedPoint.of(bufferAllocation.getBtreeAddress()),
                        HdfFixedPoint.of(fileAllocation.getBtreeOffset()),
                        // HdfFixedPoint.of(bufferAllocation.getLocalHeapAddress())));
                        HdfFixedPoint.of(fileAllocation.getLocalHeapOffset())));

        // rootGroup = new HdfGroup(this, "", bufferAllocation.getBtreeAddress(), bufferAllocation.getLocalHeapAddress());
        rootGroup = new HdfGroup(this, "", fileAllocation.getBtreeOffset(), fileAllocation.getLocalHeapOffset());
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
        return rootGroup.createDataSet(datasetName, hdfDatatype, dataSpaceMessage);
    }

//    protected void recomputeGlobalHeapAddress(HdfDataSet dataSet) {
//        HdfFixedPoint dimensionSize = dataSet.getDataObjectHeaderPrefix().findMessageByType(DataLayoutMessage.class).orElseThrow().getDimensionSizes()[0];
//        // bufferAllocation.computeGlobalHeapAddress(dimensionSize.getInstance(Long.class));
//        fileAllocation.computeGlobalHeapOffset(dimensionSize.getInstance(Long.class));
//    }
//
    public void write(Supplier<ByteBuffer> bufferSupplier, HdfDataSet hdfDataSet) throws IOException {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        DatasetAllocationInfo allocationInfo = fileAllocation.getDatasetAllocationInfo(hdfDataSet.getDatasetName());
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            // fileChannel.position(bufferAllocation.getDataAddress());
            fileChannel.position(allocationInfo.getDataOffset());
            ByteBuffer buffer;
            while ((buffer = bufferSupplier.get()).hasRemaining()) {
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
            }
        }
    }

    public void write(ByteBuffer buffer, HdfDataSet hdfDataSet) throws IOException {
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        long dataOffset = fileAllocation.allocateAndSetDataBlock(hdfDataSet.getDatasetName(), buffer.limit());
        try (FileChannel fileChannel = FileChannel.open(Path.of(fileName), openOptions)) {
            // fileChannel.position(bufferAllocation.getDataAddress());
            fileChannel.position(dataOffset);
            while (buffer.hasRemaining()) {
                fileChannel.write(buffer);
            }
        }
    }

    @Override
    public void close() throws IOException {
        rootGroup.close();
        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        // long endOfFileAddress = bufferAllocation.getDataAddress();
        long endOfFileAddress = fileAllocation.getEndOfFileOffset();
//        HdfFixedPoint[] dimensionSizes = rootGroup.getDataSet().getDataObjectHeaderPrefix()
//                .findMessageByType(DataLayoutMessage.class)
//                .orElseThrow()
//                .getDimensionSizes();
//        for(HdfFixedPoint fixedPoint : dimensionSizes) {
//            endOfFileAddress += fixedPoint.getInstance(Long.class);
//        }
//
//        // some convoluted logic for adding globalHeap data if needed
//        ByteBuffer globalHeapBuffer = null;
//        long globalHeapAddress = -1;
//        long globalHeapSize = globalHeap.getWriteBufferSize(-1);
//        if ( globalHeapSize > 0 ) {
//            globalHeapAddress = endOfFileAddress;
//            endOfFileAddress += globalHeapSize;
//            globalHeapBuffer = ByteBuffer.allocate((int) globalHeapSize);
//            globalHeap.writeToByteBuffer(globalHeapBuffer);
//            globalHeapBuffer.position(0);
//        }
        superblock.setEndOfFileAddress(HdfFixedPoint.of(endOfFileAddress));

        Path path = Path.of(fileName);
        StandardOpenOption[] fileOptions = {StandardOpenOption.WRITE};
        if ( !Files.exists(path) ) {
            fileOptions =new StandardOpenOption[]{StandardOpenOption.WRITE, StandardOpenOption.CREATE};
        }

        try (FileChannel fileChannel = FileChannel.open(path, fileOptions)) {
            // write super block
            log.debug("{}", superblock);
            superblock.writeToFileChannel(fileChannel);

            // write root group, writes all dataset and snod allocations as well.
            log.debug("{}", rootGroup);
            rootGroup.writeToFileChannel(fileChannel);

//            // check here if global heap needs to be written
//            if ( globalHeapAddress > 0 ) {
//                fileChannel.position(globalHeapAddress);
//                while (globalHeapBuffer.hasRemaining()) {
//                    fileChannel.write(globalHeapBuffer);
//                }
//            }

        }


    }

}
