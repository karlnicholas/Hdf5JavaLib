package org.hdf5javalib.redo.hdffile.infrastructure;

public class HdfLocalHeapDataValue {
    private final String value;
    private final Long offset;

    public HdfLocalHeapDataValue(String value, Long offset) {
        this.value = value;
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public Long getOffset() {
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
