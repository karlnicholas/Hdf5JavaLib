package com.github.karlnicholas.hdf5javalib.datatype;

import lombok.Getter;

@Getter
public class HdfCompoundDatatypeMember {
    private final String name;
    private final int offset;
    protected final int dimensionality;
    protected final int dimensionPermutation;
    protected final int[] dimensionSizes;
    protected final HdfDatatype type;

    public HdfCompoundDatatypeMember(String name, int offset, int dimensionality, int dimensionPermutation, int[] dimensionSizes, HdfDatatype type) {
        this.name = name;
        this.offset = offset;
        this.dimensionality = dimensionality;
        this.dimensionPermutation = dimensionPermutation;
        this.dimensionSizes = dimensionSizes;
        this.type = type;
    }

    @Override
    public String toString() {
        return "Member{" +
                "name='" + name + '\'' +
                ", offset=" + offset +
                ", dimensionality=" + dimensionality +
                ", dimensionPermutation=" + dimensionPermutation +
                ", dimensionSizes=" + java.util.Arrays.toString(dimensionSizes) +
                ", type=" + type +
                '}';
    }

}
