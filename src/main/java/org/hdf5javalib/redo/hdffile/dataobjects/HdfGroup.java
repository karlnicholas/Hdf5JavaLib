package org.hdf5javalib.redo.hdffile.dataobjects;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;
import org.hdf5javalib.redo.dataclass.HdfString;
import org.hdf5javalib.redo.datatype.HdfDatatype;
import org.hdf5javalib.redo.datatype.StringDatatype;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.HdfFileAllocation;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.DataspaceMessage;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.SymbolTableMessage;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfBTreeSnodEntry;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfBTreeV1;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfLocalHeap;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfSymbolTableEntryCacheNotUsed;
import org.hdf5javalib.redo.utils.HdfWriteUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hdf5javalib.redo.hdffile.HdfFileAllocation.*;

/**
 * Represents an HDF5 group within an HDF5 file.
 * <p>
 * The {@code HdfGroup} class manages a group, which is a container for datasets and
 * other groups in an HDF5 file. It handles the group's metadata, including its B-tree
 * for symbol table entries, local heap for link names, and object header. This class
 * supports creating datasets, writing group data to a file channel, and closing
 * associated resources. It implements {@link Closeable} to ensure proper resource
 * management.
 * </p>
 */
public class HdfGroup implements HdfDataObject, Closeable {
    private final HdfDataFile hdfDataFile;
    /**
     * The name of the group.
     */
    private final String groupName;
    /**
     * The object header prefix for the group.
     */
    private final HdfObjectHeaderPrefixV1 objectHeader;
    /**
     * The B-tree managing symbol table entries.
     */
    private final HdfBTreeV1 bTree;
    /**
     * The local heap storing link names.
     */
    private final HdfLocalHeap localHeap;

    public HdfBTreeV1 getBTree() {
        return bTree;
    }

    public Optional<HdfBTreeV1> getBTreeOptionally() {
        return Optional.of(bTree);
    }

    public HdfLocalHeap getLocalHeap() {
        return localHeap;
    }

    public HdfObjectHeaderPrefixV1 getObjectHeader() {
        return objectHeader;
    }

    public String getGroupName() {
        return groupName;
    }

    /**
     * Constructs an HdfGroup for reading an existing HDF5 file.
     * <p>
     * Initializes the group with metadata and datasets parsed from the file, including
     * the object header, B-tree, local heap, and dataset map.
     * </p>
     *
     * @param groupName    the name of the group
     * @param objectHeader the object header prefix containing group metadata
     * @param bTree        the B-tree managing symbol table entries
     * @param localHeap    the local heap storing link names
     */
    public HdfGroup(
            String groupName,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap,
            HdfDataFile hdfDataFile
    ) {
        this.groupName = groupName;
        this.objectHeader = objectHeader;
        this.bTree = bTree;
        this.localHeap = localHeap;
        this.hdfDataFile = hdfDataFile;
    }

    /**
     * Constructs an HdfGroup for creating a new HDF5 file.
     * <p>
     * Initializes the group with a new B-tree, local heap, and empty dataset map,
     * setting up the necessary metadata for writing to the file.
     * </p>
     *
     * @param groupName        the name of the group
     * @param btreeAddress     the file address for the B-tree
     * @param localHeapAddress the file address for the local heap
     */
    public HdfGroup(String groupName, long btreeAddress, long localHeapAddress, HdfDataFile hdfDataFile) {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        this.groupName = groupName;
        this.hdfDataFile = hdfDataFile;
        HdfFixedPoint localHeapContentsSize = fileAllocation.getCurrentLocalHeapContentsSize();

        localHeap = new HdfLocalHeap(
                localHeapContentsSize,
                fileAllocation.getCurrentLocalHeapContentsOffset(),
                hdfDataFile, groupName + "heap",
                HdfFileAllocation.SUPERBLOCK_SIZE + HdfFileAllocation.OBJECT_HEADER_PREFIX_SIZE + BTREE_NODE_SIZE + BTREE_STORAGE_SIZE
        );

        localHeap.addToHeap("");

        bTree = new HdfBTreeV1(0, 0,
                hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().undefined(),
                hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset().undefined(),
                hdfDataFile,
                groupName + "btree",
                HdfWriteUtils.hdfFixedPointFromValue(SUPERBLOCK_OFFSET + SUPERBLOCK_SIZE + OBJECT_HEADER_PREFIX_SIZE, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset())
        );

        HdfFixedPoint btree = HdfWriteUtils.hdfFixedPointFromValue(btreeAddress, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset());
        HdfFixedPoint localHeap = HdfWriteUtils.hdfFixedPointFromValue(localHeapAddress, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset());

        objectHeader = new HdfObjectHeaderPrefixV1(1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(btree, localHeap, (byte) 0, (short) (btree.getDatatype().getSize() + localHeap.getDatatype().getSize()))),
                hdfDataFile,
                groupName + "header",
                HdfWriteUtils.hdfFixedPointFromValue(SUPERBLOCK_OFFSET + SUPERBLOCK_SIZE + OBJECT_HEADER_PREFIX_SIZE + BTREE_NODE_SIZE + BTREE_STORAGE_SIZE, hdfDataFile.getFileAllocation().getSuperblock().getFixedPointDatatypeForOffset())
        );
    }

    /**
     * Creates a dataset in the group.
     * <p>
     * Allocates storage for the dataset, adds its name to the local heap, and updates
     * the B-tree with the dataset's metadata. The dataset is added to the group's dataset map.
     * </p>
     *
     * @param hdfDataFile      the HDF5 file context
     * @param datasetName      the name of the dataset
     * @param hdfDatatype      the datatype of the dataset
     * @param dataSpaceMessage the dataspace message defining the dataset's dimensions
     * @return the created {@link HdfDataSet}
     */
    public HdfDataSet createDataSet(HdfDataFile hdfDataFile, String datasetName, HdfDatatype hdfDatatype, DataspaceMessage dataSpaceMessage) {
        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), datasetName.getBytes().length, hdfDataFile));
        HdfFixedPoint linkNameOffset;
        linkNameOffset = localHeap.addToHeap(hdfDatasetName.toString());

        HdfDataSet newDataSet = new HdfDataSet(hdfDataFile, datasetName, hdfDatatype, dataSpaceMessage);

        bTree.addDataset(linkNameOffset, newDataSet, this);
        return newDataSet;
    }

    /**
     * Retrieves the dataset name associated with a given link name offset.
     *
     * @param linkNameOffset the link name offset to look up
     * @return the dataset name associated with the offset
     * @throws IllegalArgumentException if the offset is not found
     */
    public String getDatasetNameByLinkNameOffset(HdfFixedPoint linkNameOffset) {
        return localHeap.stringAtOffset(linkNameOffset);
    }

    /**
     * Writes the group's metadata and associated data to a file channel.
     *
     * @param seekableByteChannel the file channel to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeToFileChannel(SeekableByteChannel seekableByteChannel) throws IOException {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        objectHeader.writeAsGroupToByteChannel(seekableByteChannel, fileAllocation);
        bTree.writeToByteChannel(seekableByteChannel, fileAllocation);
        localHeap.writeToByteChannel(seekableByteChannel, fileAllocation);
    }

    /**
     * Retrieves all datasets in the group.
     *
     * @return a collection of all {@link org.hdf5javalib.file.HdfDataSet} objects in the group
     */
    public List<HdfDataSet> getDataSets() {
        return bTree.getEntries().stream()
                .filter(bte -> bte instanceof HdfBTreeSnodEntry)
                .flatMap(bte -> ((HdfBTreeSnodEntry) bte).getSymbolTableNode().getSymbolTableEntries().stream())
                .filter(ste -> ste.getCache() instanceof HdfSymbolTableEntryCacheNotUsed)
                .map(ste -> ((HdfSymbolTableEntryCacheNotUsed) ste.getCache()).getDataSet())
                .toList();

    }

//    public HdfDataSet findDataset(String datasetName) {
//        return bTree.getEntries().stream()
//                .filter(bte -> bte instanceof HdfBTreeSnodEntry)
//                .flatMap(bte -> ((HdfBTreeSnodEntry) bte).getSymbolTableNode().getSymbolTableEntries().stream())
//                .filter(ste -> ste.getCache() instanceof HdfSymbolTableEntryCacheNotUsed)
//                .map(ste -> ((HdfSymbolTableEntryCacheNotUsed) ste.getCache()).getDataSet())
//                .filter(dataSet -> dataSet.getDatasetName().equalsIgnoreCase(datasetName))
//                .findFirst()
//                .orElse(null);
//    }

    /**
     * Returns a string representation of the HdfGroup.
     *
     * @return a string describing the group's name, object header, B-tree, local heap, and datasets
     */
    @Override
    public String toString() {
        return "HdfGroup{" +
                "name='" + groupName + '\'' +
                "\r\nobjectHeader=" + objectHeader +
                "\r\nbTree=" + bTree +
                "\r\nlocalHeap=" + localHeap +
                "}";
    }

    /**
     * Closes the group and all its datasets.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
    }

    @Override
    public String getObjectName() {
        return groupName;
    }

    private Optional<HdfDataObject> findTypeInBTree(HdfBTreeV1 bTree, String[] components, int index, String currentComponent) {
        if (bTree == null || components == null || index >= components.length || currentComponent == null) {
            return Optional.empty();
        }
        Optional<HdfDataObject> result = bTree.findObjectByName(currentComponent, this);
        if (result.isPresent()) {
            return findTypeByPath(components, index + 1, result.get());
        }
        return Optional.empty();
    }

    private Optional<HdfDataObject> findTypeByPath(String[] components, int index, HdfDataObject currentInstance) {
        if (index >= components.length || components[index].isEmpty() || currentInstance == null) {
            return Optional.empty();
        }
        String currentComponent = components[index];
        if (index == components.length - 1) {
            if (currentInstance.getObjectName().equals(currentComponent)) {
                return Optional.of(currentInstance);
            }
            return currentInstance.getBTreeOptionally()
                    .flatMap(bTree -> bTree.findObjectByName(currentComponent, (HdfGroup) currentInstance));
        }
        return currentInstance.getBTreeOptionally()
                .flatMap(bTree -> findTypeInBTree(bTree, components, index, currentComponent));
    }

    public Optional<HdfDataSet> getDataset(String path) {
        return getObjectByPath(path, HdfDataSet.class);
    }

    public Optional<HdfGroup> getGroup(String path) {
        return getObjectByPath(path, HdfGroup.class);
    }

    private <T extends HdfDataObject> Optional<T> getObjectByPath(String path, Class<T> type) {
        if (path == null || path.isEmpty()) {
            return Optional.empty();
        }
        String cleanedPath = path.startsWith("/") ? path.substring(1) : path;
        if (cleanedPath.isEmpty()) {
            return Optional.empty();
        }
        String[] components = cleanedPath.split("/");
        if (components.length == 0) {
            return Optional.empty();
        }
        return findTypeByPath(components, 0, this)
                .filter(type::isInstance)
                .map(type::cast);
    }
}