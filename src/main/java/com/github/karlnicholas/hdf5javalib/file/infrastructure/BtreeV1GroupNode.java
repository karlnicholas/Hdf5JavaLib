package com.github.karlnicholas.hdf5javalib.file.infrastructure;

import com.github.karlnicholas.hdf5javalib.datatype.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.datatype.HdfString;
import lombok.Getter;

@Getter
public class BtreeV1GroupNode {
    private final HdfString objectName;
    private final HdfFixedPoint dataAddress;

    public BtreeV1GroupNode(HdfString objectName, HdfFixedPoint dataAddress) {
        this.objectName = objectName;
        this.dataAddress = dataAddress;
    }

    @Override
    public String toString() {
        return "BtreeV1GroupNode [objectName=" + objectName + ", dataAddress=" + dataAddress + "]";
    }
}
