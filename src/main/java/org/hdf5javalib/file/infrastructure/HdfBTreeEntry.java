package org.hdf5javalib.file.infrastructure;

import lombok.Data;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.math.BigInteger;

@Data
public class HdfBTreeEntry {
    private final HdfFixedPoint<BigInteger> key;
    private final HdfFixedPoint<BigInteger> childPointer;
}
