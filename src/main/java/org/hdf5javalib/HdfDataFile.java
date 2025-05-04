package org.hdf5javalib;

import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.channels.SeekableByteChannel;

/**
 * Interface for accessing core components of an HDF5 file.
 * <p>
 * The {@code HdfDataFile} interface defines methods for retrieving essential
 * components of an HDF5 file, including the global heap, file allocation manager,
 * byte channel, and fixed-point datatypes for offset and length fields. Implementations
 * of this interface provide the necessary context for reading and writing HDF5 data.
 * </p>
 */
public interface HdfDataFile {
    /**
     * Retrieves the global heap of the HDF5 file.
     *
     * @return the {@link HdfGlobalHeap} instance
     */
    HdfGlobalHeap getGlobalHeap();

    /**
     * Retrieves the file allocation manager of the HDF5 file.
     *
     * @return the {@link HdfFileAllocation} instance
     */
    HdfFileAllocation getFileAllocation();

    /**
     * Retrieves the seekable byte channel for reading and writing the HDF5 file.
     *
     * @return the {@link SeekableByteChannel} instance
     */
    SeekableByteChannel getSeekableByteChannel();

    /**
     * Retrieves the fixed-point datatype used for offset fields in the HDF5 file.
     *
     * @return the {@link FixedPointDatatype} for offsets
     */
    FixedPointDatatype getFixedPointDatatypeForOffset();

    /**
     * Retrieves the fixed-point datatype used for length fields in the HDF5 file.
     *
     * @return the {@link FixedPointDatatype} for lengths
     */
    FixedPointDatatype getFixedPointDatatypeForLength();
}