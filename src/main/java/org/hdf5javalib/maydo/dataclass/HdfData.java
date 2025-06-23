package org.hdf5javalib.maydo.dataclass;

import java.nio.ByteBuffer;

/**
 * Interface for HDF5 data objects.
 * <p>
 * The {@code HdfData} interface defines the contract for classes that represent
 * data structures in an HDF5 file. Implementing classes encapsulate raw byte data
 * and associated datatypes, providing methods to write the data to a buffer and
 * convert it to various Java types.
 * </p>
 *
 * @see org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype
 */
public interface HdfData {
    /**
     * Writes the data object's value to the provided ByteBuffer.
     * <p>
     * Implementations should write the raw byte representation of the data to the
     * buffer, formatted according to the associated datatype's specifications
     * (e.g., endianness, offsets, padding).
     * </p>
     *
     * @param buffer the ByteBuffer to write the data to
     */
    void writeValueToByteBuffer(ByteBuffer buffer);

    /**
     * Converts the data object to an instance of the specified Java class.
     * <p>
     * Implementations should delegate to the associated datatype to convert the raw
     * byte data into the requested type, such as a primitive, array, or custom object.
     * </p>
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @return an instance of type T created from the data
     * @throws UnsupportedOperationException if the datatype cannot convert to the requested type
     */
    <T> T getInstance(Class<T> clazz);
}