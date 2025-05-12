package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.AllocationRecord;
import org.hdf5javalib.redo.dataclass.HdfFixedPoint;

import java.util.*;

public class HdfLocalHeapData extends AllocationRecord {
    private Map<HdfFixedPoint, HdfLocalHeapDataValue> data = new LinkedHashMap<>();
}
