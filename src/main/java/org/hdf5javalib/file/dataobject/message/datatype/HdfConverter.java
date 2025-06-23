package org.hdf5javalib.file.dataobject.message.datatype;

/**
 * A functional interface for converting byte data to a specific Java type using an HDF5 datatype.
 * <p>
 * The {@code DatatypeConverter} interface defines a method to convert a byte array to a target type {@code T}
 * based on the provided HDF5 datatype {@code D}. It is used to map raw HDF5 data to Java objects.
 * </p>
 *
 * @param <D> the type of HDF5 datatype, extending {@link HdfDatatype}
 * @param <T> the target Java type to convert to
 * @see org.hdf5javalib.file.dataobject.message.datatype.HdfDatatype
 */
@FunctionalInterface
public interface HdfConverter<D extends HdfDatatype, T> {
    /**
     * Converts a byte array to the target Java type using the specified HDF5 datatype.
     *
     * @param bytes    the byte array containing the raw data
     * @param datatype the HDF5 datatype used for conversion
     * @return an instance of the target type T
     */
    T convert(byte[] bytes, D datatype);
}