package com.github.karlnicholas.hdf5javalib.file;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.file.metadata.HdfSuperblock;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

    public HdfFile(String fileName, StandardOpenOption[] openOptions) {
        this.fileName = fileName;
        this.openOptions = openOptions;
        hdfGroupManager = new HdfGroupManager();
        hdfGroupManager.initializeNewHdfFile();
        messageCount = new AtomicLong();
        // 100320
        superblock(4, 16, 0, 0);

    }


    private void superblock(
            int groupLeafNodeK,
            int groupInternalNodeK,
            long baseAddress,
            long endOfFileAddress
    ) {
        this.superblock = new HdfSuperblock(0, 0, 0, 0, (short)8, (short)8, groupLeafNodeK, groupInternalNodeK,
                HdfFixedPoint.of(baseAddress),
                HdfFixedPoint.undefined((short)8),
                HdfFixedPoint.of(endOfFileAddress),
                HdfFixedPoint.undefined((short)8));
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

    public void close() {
        // placeholder. I think nothing to do.
    }
}
