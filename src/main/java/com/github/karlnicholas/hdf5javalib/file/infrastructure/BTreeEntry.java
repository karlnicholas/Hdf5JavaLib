package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import lombok.Data;

@Data
public class BTreeEntry {
    private final HdfFixedPoint key;
    private final HdfFixedPoint childPointer;
}
