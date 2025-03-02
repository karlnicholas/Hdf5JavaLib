package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.dataclass.HdfFixedPoint;
import lombok.Data;

@Data
public class HdfBTreeEntry {
    private final HdfFixedPoint key;
    private final HdfFixedPoint childPointer;
}
