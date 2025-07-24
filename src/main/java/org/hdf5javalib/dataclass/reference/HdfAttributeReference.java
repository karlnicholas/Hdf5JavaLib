package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.datatype.ReferenceDatatype;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDataHolder;

public class HdfAttributeReference implements HdfReferenceInstance {
    private final boolean external;

    public HdfAttributeReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
    }

    @Override
    public HdfDataHolder getData(HdfDataFile dataFile) {
        return HdfDataHolder.ofScalar(null);
    }
}
