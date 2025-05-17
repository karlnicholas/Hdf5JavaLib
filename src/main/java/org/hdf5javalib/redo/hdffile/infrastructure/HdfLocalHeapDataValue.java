package org.hdf5javalib.redo.hdffile.infrastructure;

public class HdfLocalHeapDataValue {
    private final String value;
    private final Integer offset;

    public HdfLocalHeapDataValue(String value, Integer offset) {
        this.value = value;
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public Integer getOffset() {
        return offset;
    }
}
