package org.hdf5javalib.file.infrastructure;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import lombok.Data;

@Data
public class HdfBTreeEntry {
    private final HdfFixedPoint key;
    private final HdfFixedPoint childPointer;
}
