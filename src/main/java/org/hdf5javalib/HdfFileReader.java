package org.hdf5javalib;

import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.HdfGroup;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.infrastructure.*;
import org.hdf5javalib.file.metadata.HdfSuperblock;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Reads and parses HDF5 file structures.
 * <p>
 * The {@code HdfFileReader} class implements {@link HdfDataFile} to provide functionality
 * for reading an HDF5 file from a {@link SeekableByteChannel}. It initializes the superblock,
 * root group, global heap, and file allocation, and constructs a hierarchy of groups and
 * datasets by parsing the file's metadata and data structures.
 * </p>
 */
public class HdfFileReader implements HdfDataFile {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HdfFileReader.class);
    /** The superblock containing metadata about the HDF5 file. */
    private HdfSuperblock superblock;

    /** The root group of the HDF5 file. */
    private HdfGroup rootGroup;

    /** The seekable byte channel for reading the HDF5 file. */
    private final SeekableByteChannel fileChannel;

    /** The global heap for storing variable-length data. */
    private final HdfGlobalHeap globalHeap;

    /** The file allocation manager for tracking storage blocks. */
    private final HdfFileAllocation fileAllocation;

    /**
     * Constructs an HdfFileReader for reading an HDF5 file.
     *
     * @param fileChannel the seekable byte channel for accessing the HDF5 file
     */
    public HdfFileReader(SeekableByteChannel fileChannel) {
        this.fileChannel = fileChannel;
        this.fileAllocation = new HdfFileAllocation();
        this.globalHeap = new HdfGlobalHeap(this::initializeGlobalHeap, this);
    }

    /**
     * Initializes the global heap at the specified offset.
     * <p>
     * Reads the global heap data from the file channel starting at the given offset
     * and configures the global heap instance.
     * </p>
     *
     * @param offset the file offset where the global heap data begins
     */
    private void initializeGlobalHeap(long offset) {
        try {
            fileChannel.position(offset);
            globalHeap.readFromFileChannel(fileChannel, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads and parses the HDF5 file structure.
     * <p>
     * Initializes the superblock, root group, B-tree, local heap, and datasets by
     * reading from the file channel. Constructs the group and dataset hierarchy and
     * returns this reader instance for further operations.
     * </p>
     *
     * @return this HdfFileReader instance
     * @throws IOException if an I/O error occurs during reading
     */
    public HdfFileReader readFile() throws IOException {
        superblock = HdfSuperblock.readFromFileChannel(fileChannel, this);
        log.debug("{}", superblock);

        long objectHeaderAddress = superblock.getRootGroupSymbolTableEntry().getObjectHeaderOffset().getInstance(Long.class);
        fileChannel.position(objectHeaderAddress);
        HdfObjectHeaderPrefixV1 objectHeader = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, this);

        long localHeapAddress = superblock.getRootGroupSymbolTableEntry().getLocalHeapOffset().getInstance(Long.class);
        fileChannel.position(localHeapAddress);
        HdfLocalHeap localHeap = HdfLocalHeap.readFromFileChannel(fileChannel, this);

        long bTreeAddress = superblock.getRootGroupSymbolTableEntry().getBTreeOffset().getInstance(Long.class);
        fileChannel.position(bTreeAddress);
        HdfBTreeV1 bTree = HdfBTreeV1.readFromFileChannel(fileChannel, this);

        Map<String, HdfGroup.DataSetInfo> datasetMap = collectDatasetsMap(fileChannel, bTree, localHeap);

        rootGroup = new HdfGroup(
                null,
                "",
                objectHeader,
                bTree,
                localHeap,
                datasetMap
        );

        log.debug("{}", rootGroup);
        log.debug("Parsing complete. NEXT: {}", fileChannel.position());

        return this;
    }

    /**
     * Collects a map of dataset names to their information from the B-tree and local heap.
     *
     * @param fileChannel the seekable byte channel for reading the file
     * @param bTree       the B-tree containing symbol table entries
     * @param localHeap   the local heap storing link names
     * @return a map of dataset names to their {@link HdfGroup.DataSetInfo}
     * @throws IOException if an I/O error occurs
     */
    private Map<String, HdfGroup.DataSetInfo> collectDatasetsMap(SeekableByteChannel fileChannel, HdfBTreeV1 bTree, HdfLocalHeap localHeap) throws IOException {
        Map<String, HdfGroup.DataSetInfo> dataSets = new LinkedHashMap<>();
        collectDatasetsRecursive(bTree, dataSets, localHeap, fileChannel);
        return dataSets;
    }

    /**
     * Recursively collects dataset information from the B-tree.
     * <p>
     * Traverses the B-tree, processing leaf nodes to extract dataset metadata and
     * recursively handling internal nodes to collect all datasets.
     * </p>
     *
     * @param currentNode the current B-tree node
     * @param dataSets    the map to store dataset information
     * @param localHeap   the local heap for link names
     * @param fileChannel the seekable byte channel for reading
     * @throws IOException if an I/O error occurs
     */
    private void collectDatasetsRecursive(HdfBTreeV1 currentNode,
                                          Map<String, HdfGroup.DataSetInfo> dataSets,
                                          HdfLocalHeap localHeap,
                                          SeekableByteChannel fileChannel) throws IOException {
        for (HdfBTreeEntry entry : currentNode.getEntries()) {
            if (entry.isLeafEntry()) {
                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
                for (HdfSymbolTableEntry ste : snod.getSymbolTableEntries()) {
                    HdfString linkName = localHeap.parseStringAtOffset(ste.getLinkNameOffset());
                    long dataObjectHeaderAddress = ste.getObjectHeaderOffset().getInstance(Long.class);
                    long linkNameOffset = ste.getLinkNameOffset().getInstance(Long.class);
                    fileChannel.position(dataObjectHeaderAddress);
                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromFileChannel(fileChannel, this);
                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow();
                    HdfDataSet dataset = new HdfDataSet(this, linkName.toString(), dataType.getHdfDatatype(), header);
                    HdfGroup.DataSetInfo dataSetInfo = new HdfGroup.DataSetInfo(dataset,
                            HdfWriteUtils.hdfFixedPointFromValue(0, getFixedPointDatatypeForOffset()),
                            linkNameOffset);
                    dataSets.put(linkName.toString(), dataSetInfo);
                }
            } else if (entry.isInternalEntry()) {
                HdfBTreeV1 childBTree = entry.getChildBTree();
                collectDatasetsRecursive(childBTree, dataSets, localHeap, fileChannel);
            }
        }
    }

    /**
     * Retrieves the global heap of the HDF5 file.
     *
     * @return the {@link HdfGlobalHeap} instance
     */
    @Override
    public HdfGlobalHeap getGlobalHeap() {
        return globalHeap;
    }

    /**
     * Retrieves the file allocation manager of the HDF5 file.
     *
     * @return the {@link HdfFileAllocation} instance
     */
    @Override
    public HdfFileAllocation getFileAllocation() {
        return fileAllocation;
    }

    /**
     * Retrieves the seekable byte channel for reading the HDF5 file.
     *
     * @return the {@link SeekableByteChannel} instance
     * @throws UnsupportedOperationException as this operation is not yet supported
     */
    @Override
    public SeekableByteChannel getSeekableByteChannel() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Retrieves the fixed-point datatype used for offset fields in the HDF5 file.
     *
     * @return the {@link FixedPointDatatype} for offsets
     */
    @Override
    public FixedPointDatatype getFixedPointDatatypeForOffset() {
        return superblock.getFixedPointDatatypeForOffset();
    }

    /**
     * Retrieves the fixed-point datatype used for length fields in the HDF5 file.
     *
     * @return the {@link FixedPointDatatype} for lengths
     */
    @Override
    public FixedPointDatatype getFixedPointDatatypeForLength() {
        return superblock.getFixedPointDatatypeForLength();
    }

    public HdfGroup getRootGroup() {
        return rootGroup;
    }
}