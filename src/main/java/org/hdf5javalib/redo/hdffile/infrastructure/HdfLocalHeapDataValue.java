package org.hdf5javalib.redo.hdffile.infrastructure;

import org.hdf5javalib.redo.dataclass.HdfFixedPoint;

public class HdfLocalHeapDataValue {
    private final String value;
    private final HdfFixedPoint offset;

    public HdfLocalHeapDataValue(String value, HdfFixedPoint offset) {
        this.value = value;
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public HdfFixedPoint getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "HdfLocalHeapDataValue{" +
                "value='" + value + '\'' +
                ", offset=" + offset +
                '}';
    }
}
