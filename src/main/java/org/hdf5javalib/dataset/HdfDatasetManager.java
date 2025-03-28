package org.hdf5javalib.dataset;

import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype;

public class HdfDatasetManager {
    private final HdfFile file;

    /**
     * Constructs an HdfDatasetManager with the specified HDF5 file.
     *
     * @param file the HDF5 file to manage datasets for
     */
    public HdfDatasetManager(HdfFile file) {
        this.file = file;
    }

    /**
     * Creates a dataset with the specified name, datatype, and dimensions.
     *
     * @param name       the name of the dataset
     * @param datatype   the datatype of the dataset elements
     * @param dimensions the dimensions of the dataset (up to 32)
     * @return the created HdfDataSet
     * @throws IllegalArgumentException if dimensions are invalid
     */
    public HdfDataSet createDataset(String name, HdfDatatype datatype, int... dimensions) {
        if (dimensions == null || dimensions.length == 0 || dimensions.length > 32) {
            throw new IllegalArgumentException("Dimensions must be between 1 and 32");
        }

        // Convert int[] to HdfFixedPoint[]
        HdfFixedPoint[] hdfDimensions = new HdfFixedPoint[dimensions.length];
        for (int i = 0; i < dimensions.length; i++) {
            hdfDimensions[i] = HdfFixedPoint.of(dimensions[i]);
        }

        // Calculate dataSpaceMessageSize
        short dataSpaceMessageSize = 8; // Base size
        for (HdfFixedPoint dim : hdfDimensions) {
            dataSpaceMessageSize += dim.getDatatype().getSize();
        }
        for (HdfFixedPoint maxDim : hdfDimensions) {
            dataSpaceMessageSize += maxDim.getDatatype().getSize();
        }

        // Create DataspaceMessage with N dimensions
        DataspaceMessage dataSpaceMessage = new DataspaceMessage(
                1,                              // Version
                (byte) dimensions.length,      // Number of dimensions
                DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false), // Flags
                hdfDimensions,                 // Dimensions
                hdfDimensions,                 // Max dimensions (same as dims for simplicity)
                false,                         // Permutation indices flag
                (byte) 0,                      // Reserved
                dataSpaceMessageSize           // Total size
        );

        return file.createDataSet(name, datatype, dataSpaceMessage);
    }
}