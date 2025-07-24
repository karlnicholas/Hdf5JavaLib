package org.hdf5javalib.hdffile.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;

/**
 * Represents a value stored in an HDF5 local heap with its associated offset.
 * <p>
 * This record holds a string value and its corresponding offset in the local heap,
 * used for managing variable-length data such as link names in HDF5 groups.
 * </p>
 *
 * @param value  the string value stored in the local heap
 * @param offset the offset in the local heap where the value is stored
 */
public record HdfLocalHeapDataValue(String value, HdfFixedPoint offset) {
}