package org.hdf5javalib.file;

import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import org.hdf5javalib.file.metadata.HdfSuperblock;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;

/**
 * Represents an HDF5 file and provides methods for creating and managing datasets.
 * <p>
 * The {@code HdfFile} class implements {@link HdfDataFile} and {@link Closeable} to manage
 * the lifecycle of an HDF5 file. It initializes the superblock, root group, global heap,
 * and file allocation, and provides methods for creating datasets and writing the file
 * structure to a {@link SeekableByteChannel}. This class serves as the main entry point
 * for interacting with an HDF5 file.
 * </p>
 */
public class HdfFile implements Closeable, HdfDataFile {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HdfFile.class);
    /** The superblock containing metadata about the HDF5 file. */
    private final HdfSuperblock superblock;
    /** The root group of the HDF5 file. */
    private final HdfGroup rootGroup;
    /** The global heap for storing variable-length data. */
    private final HdfGlobalHeap globalHeap;
    /** The file allocation manager for tracking storage blocks. */
    private final HdfFileAllocation fileAllocation;
    /** The seekable byte channel for reading and writing the file. */
    private final SeekableByteChannel seekableByteChannel;
    /** Indicates whether the file is closed. */
    private boolean closed;

    /**
     * Constructs a new HDF5 file.
     * <p>
     * Initializes the superblock, root group, global heap, and file allocation manager.
     * Configures the fixed-point datatypes for offsets and lengths, and sets up the
     * initial file structure.
     * </p>
     *
     * @param seekableByteChannel the seekable byte channel for file I/O
     */
    public HdfFile(SeekableByteChannel seekableByteChannel) {
        closed = false;
        this.seekableByteChannel = seekableByteChannel;
        this.fileAllocation = new HdfFileAllocation();
        this.globalHeap = new HdfGlobalHeap(this);
        FixedPointDatatype fixedPointDatatypeForOffset = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                8, (short) 0, (short) (8 * 8));
        FixedPointDatatype fixedPointDatatypeForLength = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                8, (short) 0, (short) (8 * 8));

        superblock = new HdfSuperblock(0, 0, 0, 0,
                4, 16,
                HdfWriteUtils.hdfFixedPointFromValue(0, fixedPointDatatypeForOffset),
                fixedPointDatatypeForOffset.undefined(),
                HdfWriteUtils.hdfFixedPointFromValue(0, fixedPointDatatypeForOffset),
                fixedPointDatatypeForOffset.undefined(),
                new HdfSymbolTableEntry(
                        HdfWriteUtils.hdfFixedPointFromValue(0, fixedPointDatatypeForOffset),
                        HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getObjectHeaderPrefixRecord().getOffset(), fixedPointDatatypeForOffset),
                        HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getBtreeRecord().getOffset(), fixedPointDatatypeForOffset),
                        HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getLocalHeapOffset(), fixedPointDatatypeForOffset)),
                this, fixedPointDatatypeForOffset, fixedPointDatatypeForLength);

        rootGroup = new HdfGroup(this, "", fileAllocation.getBtreeRecord().getOffset(), fileAllocation.getLocalHeapOffset());
    }

    /**
     * Creates a dataset in the root group.
     *
     * @param datasetName      the name of the dataset
     * @param hdfDatatype      the datatype of the dataset
     * @param dataSpaceMessage the dataspace message defining the dataset's dimensions
     * @return the created {@link HdfDataSet}
     */
    public HdfDataSet createDataSet(String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        hdfDatatype.setGlobalHeap(globalHeap);
        return rootGroup.createDataSet(this, datasetName, hdfDatatype, dataSpaceMessage);
    }

    /**
     * Closes the HDF5 file, writing all necessary data to the file channel.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        rootGroup.close();
        long endOfFileAddress = fileAllocation.getEndOfFileOffset();
        superblock.setEndOfFileAddress(
                HdfWriteUtils.hdfFixedPointFromValue(endOfFileAddress, getFixedPointDatatypeForOffset()));

        // Write superblock
        log.debug("{}", superblock);
        superblock.writeToFileChannel(seekableByteChannel);

        // Write root group and associated datasets
        log.debug("{}", rootGroup);
        rootGroup.writeToFileChannel(seekableByteChannel);

        // Write global heap
        getGlobalHeap().writeToFileChannel(seekableByteChannel);

        closed = true;
    }

    /**
     * Retrieves the fixed-point datatype used for offset fields.
     *
     * @return the {@link FixedPointDatatype} for offsets
     */
    @Override
    public FixedPointDatatype getFixedPointDatatypeForOffset() {
        return superblock.getFixedPointDatatypeForOffset();
    }

    /**
     * Retrieves the fixed-point datatype used for length fields.
     *
     * @return the {@link FixedPointDatatype} for lengths
     */
    @Override
    public FixedPointDatatype getFixedPointDatatypeForLength() {
        return superblock.getFixedPointDatatypeForLength();
    }

    @Override
    public HdfFileAllocation getFileAllocation() {
        return fileAllocation;
    }

    @Override
    public SeekableByteChannel getSeekableByteChannel() {
        return seekableByteChannel;
    }

    @Override
    public HdfGlobalHeap getGlobalHeap() {
        return globalHeap;
    }
}