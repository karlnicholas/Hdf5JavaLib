package org.hdf5javalib.file;

import lombok.Getter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.SymbolTableMessage;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.file.infrastructure.HdfBTreeV1;
import org.hdf5javalib.file.infrastructure.HdfLocalHeap;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
@Getter
public class HdfGroup implements Closeable {
    /** The HDF5 file context. */
    private final HdfFile hdfFile;
    /** The name of the group. */
    private final String name;
    /** The object header prefix for the group. */
    private final HdfObjectHeaderPrefixV1 objectHeader;
    /** The B-tree managing symbol table entries. */
    private final HdfBTreeV1 bTree;
    /** The local heap storing link names. */
    private final HdfLocalHeap localHeap;
    /** Map of dataset names to their information (dataset, header offset, link name offset). */
    private final Map<String, DataSetInfo> dataSets;

    /**
     * Inner class to hold dataset information, including the dataset, header offset,
     * and link name offset.
     */
    public static class DataSetInfo {
        private final HdfDataSet dataSet;
        private final HdfFixedPoint headerOffset;
        private final long linkNameOffset;

        /**
         * Constructs a DataSetInfo instance.
         *
         * @param dataSet        the dataset
         * @param headerOffset   the offset of the dataset's header
         * @param linkNameOffset the offset of the dataset's link name in the local heap
         */
        public DataSetInfo(HdfDataSet dataSet, HdfFixedPoint headerOffset, long linkNameOffset) {
            this.dataSet = dataSet;
            this.headerOffset = headerOffset;
            this.linkNameOffset = linkNameOffset;
        }

        /**
         * Retrieves the dataset.
         *
         * @return the {@link HdfDataSet} instance
         */
        public HdfDataSet getDataSet() {
            return dataSet;
        }

        /**
         * Retrieves the header offset of the dataset.
         *
         * @return the {@link HdfFixedPoint} representing the header offset
         */
        public HdfFixedPoint getHeaderOffset() {
            return headerOffset;
        }

        /**
         * Retrieves the link name offset in the local heap.
         *
         * @return the offset of the dataset's link name
         */
        public long getLinkNameOffset() {
            return linkNameOffset;
        }
    }

    /**
     * Constructs an HdfGroup for reading an existing HDF5 file.
     * <p>
     * Initializes the group with metadata and datasets parsed from the file, including
     * the object header, B-tree, local heap, and dataset map.
     * </p>
     *
     * @param hdfFile      the HDF5 file containing this group
     * @param name         the name of the group
     * @param objectHeader the object header prefix containing group metadata
     * @param bTree        the B-tree managing symbol table entries
     * @param localHeap    the local heap storing link names
     * @param dataSets     a map of dataset names to their corresponding DataSetInfo
     */
    public HdfGroup(
            HdfFile hdfFile,
            String name,
            HdfObjectHeaderPrefixV1 objectHeader,
            HdfBTreeV1 bTree,
            HdfLocalHeap localHeap,
            Map<String, DataSetInfo> dataSets
    ) {
        this.hdfFile = hdfFile;
        this.name = name;
        this.objectHeader = objectHeader;
        this.bTree = bTree;
        this.localHeap = localHeap;
        this.dataSets = dataSets;
    }

    /**
     * Constructs an HdfGroup for creating a new HDF5 file.
     * <p>
     * Initializes the group with a new B-tree, local heap, and empty dataset map,
     * setting up the necessary metadata for writing to the file.
     * </p>
     *
     * @param hdfFile         the HDF5 file to be written
     * @param name            the name of the group
     * @param btreeAddress    the file address for the B-tree
     * @param localHeapAddress the file address for the local heap
     */
    public HdfGroup(HdfFile hdfFile, String name, long btreeAddress, long localHeapAddress) {
        HdfFileAllocation fileAllocation = hdfFile.getFileAllocation();
        this.hdfFile = hdfFile;
        this.name = name;
        long localHeapContentsSize = fileAllocation.getCurrentLocalHeapContentsSize();
        byte[] heapData = new byte[(int) localHeapContentsSize];
        heapData[0] = (byte)0x1;
        heapData[8] = (byte)localHeapContentsSize;

        localHeap = new HdfLocalHeap(
                HdfWriteUtils.hdfFixedPointFromValue(localHeapContentsSize, hdfFile.getFixedPointDatatypeForLength()),
                HdfWriteUtils.hdfFixedPointFromValue(fileAllocation.getCurrentLocalHeapContentsOffset(), hdfFile.getFixedPointDatatypeForOffset()),
                hdfFile);

        localHeap.addToHeap(
                new HdfString(new byte[0],
                        new StringDatatype(
                                StringDatatype.createClassAndVersion(),
                                StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), 0))
        );

        bTree = new HdfBTreeV1("TREE", 0, 0,
                hdfFile.getFixedPointDatatypeForOffset().undefined(),
                hdfFile.getFixedPointDatatypeForOffset().undefined(),
                hdfFile);

        HdfFixedPoint btree = HdfWriteUtils.hdfFixedPointFromValue(btreeAddress, hdfFile.getFixedPointDatatypeForOffset());
        HdfFixedPoint localHeap = HdfWriteUtils.hdfFixedPointFromValue(localHeapAddress, hdfFile.getFixedPointDatatypeForOffset());

        objectHeader = new HdfObjectHeaderPrefixV1(1, 1, 24,
                Collections.singletonList(new SymbolTableMessage(btree, localHeap, (byte)0, (short) (btree.getDatatype().getSize() + localHeap.getDatatype().getSize()))));

        this.dataSets = new LinkedHashMap<>();
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
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        HdfString hdfDatasetName = new HdfString(datasetName.getBytes(), new StringDatatype(StringDatatype.createClassAndVersion(), StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII), datasetName.getBytes().length));
        int linkNameOffset;
        long allocationInfo;
        if (localHeap.getFreeListOffset().getInstance(Long.class) != 1) {
            linkNameOffset = localHeap.addToHeap(hdfDatasetName);
            allocationInfo = fileAllocation.allocateDatasetStorage(datasetName);
        } else {
            allocationInfo = fileAllocation.allocateDatasetStorage(datasetName);
            linkNameOffset = localHeap.addToHeap(hdfDatasetName);
        }

        HdfDataSet newDataSet = new HdfDataSet(hdfDataFile, datasetName, hdfDatatype, dataSpaceMessage);

        DataSetInfo dataSetInfo = new DataSetInfo(
                newDataSet,
                HdfWriteUtils.hdfFixedPointFromValue(allocationInfo, hdfFile.getFixedPointDatatypeForOffset()),
                linkNameOffset);
        dataSets.put(datasetName, dataSetInfo);

        bTree.addDataset(linkNameOffset, allocationInfo, datasetName, this);
        return newDataSet;
    }

    /**
     * Retrieves the dataset name associated with a given link name offset.
     *
     * @param linkNameOffset the link name offset to look up
     * @return the dataset name associated with the offset
     * @throws IllegalArgumentException if the offset is not found
     */
    public String getDatasetNameByLinkNameOffset(long linkNameOffset) {
        for (Map.Entry<String, DataSetInfo> entry : dataSets.entrySet()) {
            if (entry.getValue().getLinkNameOffset() == linkNameOffset) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException("Link name offset " + linkNameOffset + " not found in group " + name);
    }

    /**
     * Writes the group's metadata and associated data to a file channel.
     *
     * @param seekableByteChannel the file channel to write to
     * @throws IOException if an I/O error occurs
     */
    public void writeToFileChannel(SeekableByteChannel seekableByteChannel) throws IOException {
        HdfFileAllocation fileAllocation = hdfFile.getFileAllocation();
        objectHeader.writeAsGroupToByteChannel(seekableByteChannel, fileAllocation);
        bTree.writeToByteChannel(seekableByteChannel, fileAllocation);
        localHeap.writeToByteChannel(seekableByteChannel, fileAllocation);
    }

    /**
     * Retrieves all datasets in the group.
     *
     * @return a collection of all {@link HdfDataSet} objects in the group
     */
    public Collection<HdfDataSet> getDataSets() {
        return dataSets.values().stream().map(DataSetInfo::getDataSet).collect(Collectors.toList());
    }

    /**
     * Finds a dataset by name.
     *
     * @param datasetName the name of the dataset to find
     * @return the {@link HdfDataSet} with the specified name, or null if not found
     */
    public HdfDataSet findDataset(String datasetName) {
        DataSetInfo info = dataSets.get(datasetName);
        return info != null ? info.getDataSet() : null;
    }

    /**
     * Returns a string representation of the HdfGroup.
     *
     * @return a string describing the group's name, object header, B-tree, local heap, and datasets
     */
    @Override
    public String toString() {
        String dataSetsString = dataSets.isEmpty()
                ? ""
                : "\r\ndataSets=[\r\n" + dataSets.entrySet().stream()
                .map(entry -> "  " + entry.getKey() + "@" + entry.getValue().getHeaderOffset() + " " + entry.getValue().getDataSet().getDataObjectHeaderPrefix())
                .collect(Collectors.joining(",\r\n")) + "\r\n]";

        return "HdfGroup{" +
                "name='" + name + '\'' +
                "\r\nobjectHeader=" + objectHeader +
                "\r\nbTree=" + bTree +
                "\r\nlocalHeap=" + localHeap +
                dataSetsString +
                "}";
    }

    /**
     * Closes the group and all its datasets.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        for (DataSetInfo datasetInfo : dataSets.values()) {
            datasetInfo.getDataSet().close();
        }
    }
}