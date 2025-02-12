package com.github.karlnicholas.hdf5javalib;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;
import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;

@Getter
public class H5File {
    private final String fileName;
    private final StandardOpenOption[] fileOptions;
    public H5File(String fileName, StandardOpenOption... fileOptions) {
        this.fileName = fileName;
        this.fileOptions = fileOptions;
    }

    public HdfDataSet createDataSet(String datasetName, CompoundDataType compoundType, HdfFixedPoint[] hdfDimensions) {
        return new HdfDataSet(this);
    }

    public void close() {
    }

    public void write(ByteBuffer buffer) {
    }
}
