package org.hdf5javalib;

import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.channels.SeekableByteChannel;

public interface HdfDataFile {
    HdfGlobalHeap getGlobalHeap();
    HdfFileAllocation getFileAllocation();
    SeekableByteChannel getSeekableByteChannel();
    FixedPointDatatype getFixedPointDatatypeForOffset();
    FixedPointDatatype getFixedPointDatatypeForLength();
}
